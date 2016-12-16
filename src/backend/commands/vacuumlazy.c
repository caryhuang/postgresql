/*-------------------------------------------------------------------------
 *
 * vacuumlazy.c
 *	  Concurrent ("lazy") vacuuming.
 *
 *
 * The major space usage for LAZY VACUUM is storage for the array of dead
 * tuple TIDs, with the next biggest need being storage for per-disk-page
 * free space info.  We want to ensure we can vacuum even the very largest
 * relations with finite memory space usage.  To do that, we set upper bounds
 * on the number of tuples and pages we will keep track of at once.
 *
 * We are willing to use at most maintenance_work_mem (or perhaps
 * autovacuum_work_mem) memory space to keep track of dead tuples.  We
 * initially allocate an array of TIDs of that size, with an upper limit that
 * depends on table size (this limit ensures we don't allocate a huge area
 * uselessly for vacuuming small tables).  If the array threatens to overflow,
 * we suspend the heap scan phase and perform a pass of index cleanup and page
 * compaction, then resume the heap scan with an empty TID array.
 *
 * If we're processing a table with no indexes, we can just vacuum each page
 * as we go; there's no need to save up multiple tuples to minimize the number
 * of index scans performed.  So we don't use maintenance_work_mem memory for
 * the TID array, just enough to hold as many heap tuples as fit on one page.
 *
 * In PostgreSQL 10, we support parallel option for lazy vacuum.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuumlazy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/storage.h"
#include "commands/dbcommands.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"


/*
 * Space/time tradeoff parameters: do these need to be user-tunable?
 *
 * To consider truncating the relation, we want there to be at least
 * REL_TRUNCATE_MINIMUM or (relsize / REL_TRUNCATE_FRACTION) (whichever
 * is less) potentially-freeable pages.
 */
#define REL_TRUNCATE_MINIMUM	1000
#define REL_TRUNCATE_FRACTION	16

/*
 * Timing parameters for truncate locking heuristics.
 *
 * These were not exposed as user tunable GUC values because it didn't seem
 * that the potential for improvement was great enough to merit the cost of
 * supporting them.
 */
#define VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL		20		/* ms */
#define VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL		50		/* ms */
#define VACUUM_TRUNCATE_LOCK_TIMEOUT			5000	/* ms */

/*
 * Guesstimation of number of dead tuples per page.  This is used to
 * provide an upper limit to memory allocated when vacuuming small
 * tables.
 */
#define LAZY_ALLOC_TUPLES		MaxHeapTuplesPerPage

/*
 * Before we consider skipping a page that's marked as clean in
 * visibility map, we must've seen at least this many clean pages.
 */
#define SKIP_PAGES_THRESHOLD	((BlockNumber) 32)

/* DSM key for parallel vacuum */
#define VACUUM_KEY_PARALLEL_SCAN	50
#define VACUUM_KEY_VACUUM_STATS		51
#define VACUUM_KEY_INDEX_STATS		52
#define VACUUM_KEY_DEAD_TUPLES		53
#define VACUUM_KEY_VACUUM_TASK		54
#define VACUUM_KEY_PARALLEL_STATE	55

/*
 * see note of lazy_scan_heap_get_nextpage about forcing scanning of
 * last page
 */
#define FORCE_CHECK_PAGE(blk) \
	(blkno == (blk - 1) && should_attempt_truncation(vacrelstats))

/* Check if given index is assigned to this parallel vacuum worker */
#define IsAssignedIndex(i_num, nworkers) \
	(!IsParallelWorker() ||\
	 (IsParallelWorker() && ((i_num) % (nworkers) == ParallelWorkerNumber)))

/* Data structure for updating index relation statistics */
typedef struct LVIndStats
{
	bool		do_update;	/* Launcher process will update? */
	BlockNumber	rel_pages;	/* # of index pages */
	BlockNumber rel_tuples;	/* # of index tuples */
} LVIndStats;

#define VACSTATE_STARTUP			0x01
#define VACSTATE_SCANNING			0x02
#define VACSTATE_VACUUM_PREPARED	0x04
#define VACSTATE_VACUUMING			0x08
#define VACSTATE_VACUUM_FINISHED	0x10
#define VACSTATE_COMPLETE			0x20

/*
 * Vacuum worker state */
/*
typedef enum VacWorkerState
{
	VACSTATE_STARTUP = 0,
	VACSTATE_SCANNING,
	VACSTATE_VACUUM_PREPARED,
	VACSTATE_VACUUMING,
	VACSTATE_VACUUM_FINISHED,
	VACSTATE_COMPLETE
} VacWorkerState;
*/

typedef struct VacWorker
{
	uint8 state;
	uint32 round;
} VacWorker;

typedef struct LVParallelState
{
	int nworkers;			/* # of parallel vacuum workers */
	ConditionVariable cv;	/* condition variable for synchronization */
	slock_t	mutex;			/* mutex for vacworkers */
	VacWorker vacworkers[FLEXIBLE_ARRAY_MEMBER];
//	VacWorkerState vacstates[FLEXIBLE_ARRAY_MEMBER];
	/* vacuum workers follows */
} LVParallelState;

typedef struct LVDeadTuple
{
	int n_dt; /* # of dead tuple */
	ItemPointer dt_array;	/* NB: Each list is ordered by TID address */
} LVDeadTuple;

typedef struct LVRelStats
{
	int			nindexes; /* > 0 means two-pass strategy; = 0 means one-pass */
	/* Overall statistics about rel */
	BlockNumber old_rel_pages;	/* previous value of pg_class.relpages */
	BlockNumber rel_pages;		/* total number of pages */
	BlockNumber scanned_pages;	/* number of pages we examined */
	BlockNumber pinskipped_pages;		/* # of pages we skipped due to a pin */
	BlockNumber frozenskipped_pages;	/* # of frozen pages we skipped */
	double		scanned_tuples; /* counts only tuples on scanned pages */
	double		old_rel_tuples; /* previous value of pg_class.reltuples */
	double		new_rel_tuples; /* new estimated total # of tuples */
	double		new_dead_tuples;	/* new estimated total # of dead tuples */
	BlockNumber pages_removed;
	double		tuples_deleted;
	BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */
	/* List of TIDs of tuples we intend to delete */
	LVDeadTuple *dead_tuples;
	int			max_dead_tuples;	/* # slots allocated in array */
	int			num_index_scans;
	TransactionId latestRemovedXid;
	bool		lock_waiter_detected;
	/* Fields for parallel lazy vacuum */
	LVIndStats	*vacindstats;
	LVParallelState *pstate;
} LVRelStats;

/* Scan description data for lazy vacuum */
typedef struct LVScanDescData
{
	BlockNumber lv_cblock;					/* current scanning block number */
	BlockNumber lv_next_unskippable_block;	/* block number we should scan */
	BlockNumber lv_nblocks;					/* the number blocks of relation */
	HeapScanDesc heapscan;					/* Field for parallel lazy vacuum */
} LVScanDescData;
typedef struct LVScanDescData *LVScanDesc;

/*
 * Vacuum relevant options and thresholds we need share with parallel
 * vacuum worker.
 */
typedef struct VacuumTask
{
	int				options;
	bool			aggressive;	/* does each worker need to aggressive vacuum? */
	TransactionId	oldestxmin;
	TransactionId	freezelimit;
	MultiXactId		multixactcutoff;
	int				elevel;
} VacuumTask;

/* A few variables that don't seem worth passing around as parameters */
static int	elevel = -1;

static TransactionId OldestXmin;
static TransactionId FreezeLimit;
static MultiXactId MultiXactCutoff;

static BufferAccessStrategy vac_strategy;
static LVDeadTuple *MyDeadTuple = NULL;
static VacWorker *MyVacWorker = NULL;

/* non-export function prototypes */
static void lazy_vacuum_heap(Relation onerel, LVRelStats *vacrelstats);
static bool lazy_check_needs_freeze(Buffer buf, bool *hastup);
static void lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
				  LVRelStats *vacrelstats);
static void lazy_cleanup_index(Relation indrel,
				   IndexBulkDeleteResult *stats,
				   LVRelStats *vacrelstats,
				   LVIndStats *vacindstats);
static int lazy_vacuum_page(Relation onerel, BlockNumber blkno, Buffer buffer,
							int tupindex, LVRelStats *vacrelstats, Buffer *vmbuffer);
static bool should_attempt_truncation(LVRelStats *vacrelstats);
static void lazy_truncate_heap(Relation onerel, LVRelStats *vacrelstats);
static BlockNumber count_nondeletable_pages(Relation onerel,
						 LVRelStats *vacrelstats);
static void lazy_space_alloc(LVRelStats *vacrelstats, BlockNumber relblocks);
static void lazy_record_dead_tuple(LVRelStats *vacrelstats, ItemPointer itemptr);
static bool lazy_tid_reaped(ItemPointer itemptr, void *state);
static int	vac_cmp_itemptr(const void *left, const void *right);
static bool heap_page_is_all_visible(Relation rel, Buffer buf,
					 TransactionId *visibility_cutoff_xid, bool *all_frozen);
static void lazy_scan_heap(LVRelStats *vacrelstats, Relation onerel,
						   Relation *Irels, int nindexes,ParallelHeapScanDesc pscan,
						   int options, bool aggressive);

/* function prototypes for parallel vacuum */
static void parallel_lazy_scan_heap(Relation rel, LVRelStats *vacrelstats,
									int options, bool aggressive, int wnum);
static void lazy_vacuum_worker(dsm_segment *seg, shm_toc *toc);
static void lazy_gather_vacuum_stats(ParallelContext *pxct,
									 LVRelStats *valrelstats,
									 LVIndStats *vacindstats);
static void lazy_update_index_stats(Relation onerel, LVIndStats *vacindstats);
static void lazy_estimate_dsm(ParallelContext *pcxt, long maxtuples, int nindexes);
static void lazy_initialize_dsm(ParallelContext *pcxt, Relation onrel,
								LVRelStats *vacrelstats, int options,
								bool aggressive);
static void lazy_initialize_worker(shm_toc *toc, ParallelHeapScanDesc *pscan,
								   LVRelStats **vacrelstats, int *options,
								   bool *aggressive);
static void lazy_clear_dead_tuple(LVRelStats *vacrelstats);
static LVScanDesc lv_beginscan(LVRelStats *vacrelstats, ParallelHeapScanDesc pscan,
							   Relation onerel);
static void lv_endscan(LVScanDesc lvscan);
static BlockNumber lazy_scan_heap_get_nextpage(Relation onerel, LVRelStats* vacrelstats,
											   LVScanDesc lvscan, bool *all_visible_according_to_vm,
											   Buffer *vmbuffer, int options, bool aggressive);
static void lazy_set_vacstate_and_wait_prepared(LVParallelState *pstate);
static void lazy_set_vacstate_and_wait_finished(LVRelStats *vacrelstats);
static bool lazy_check_vacstate_prepared(LVParallelState *pstate, uint32 round);
static bool lazy_check_vacstate_finished(LVParallelState *pstate, uint32 round);
static int  lazy_count_vacstate_finished(LVParallelState *pstate, uint32 round, int *n_complete);
static uint32 lazy_set_my_vacstate(LVParallelState *pstate, uint8 state, bool nextloop,
								 bool broadcast);
static long lazy_get_max_dead_tuple(LVRelStats *vacrelstats);

/*
 *	lazy_vacuum_rel() -- perform LAZY VACUUM for one heap relation
 *
 *		This routine vacuums a single heap, cleans out its indexes, and
 *		updates its relpages and reltuples statistics.
 *
 *		At entry, we have already established a transaction and opened
 *		and locked the relation.
 */
void
lazy_vacuum_rel(Relation onerel, int options, VacuumParams *params,
				BufferAccessStrategy bstrategy)
{
	LVRelStats *vacrelstats;
	Relation   *Irel;
	int			nindexes;
	PGRUsage	ru0;
	TimestampTz starttime = 0;
	long		secs;
	int			usecs;
	double		read_rate,
				write_rate;
	bool		aggressive;		/* should we scan all unfrozen pages? */
	bool		scanned_all_unfrozen;	/* actually scanned all such pages? */
	TransactionId xidFullScanLimit;
	MultiXactId mxactFullScanLimit;
	BlockNumber new_rel_pages;
	double		new_rel_tuples;
	BlockNumber new_rel_allvisible;
	double		new_live_tuples;
	TransactionId new_frozen_xid;
	MultiXactId new_min_multi;

	Assert(params != NULL);

	/* measure elapsed time iff autovacuum logging requires it */
	if (IsAutoVacuumWorkerProcess() && params->log_min_duration >= 0)
	{
		pg_rusage_init(&ru0);
		starttime = GetCurrentTimestamp();
	}

	if (options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM,
								  RelationGetRelid(onerel));

	vac_strategy = bstrategy;

	vacuum_set_xid_limits(onerel,
						  params->freeze_min_age,
						  params->freeze_table_age,
						  params->multixact_freeze_min_age,
						  params->multixact_freeze_table_age,
						  &OldestXmin, &FreezeLimit, &xidFullScanLimit,
						  &MultiXactCutoff, &mxactFullScanLimit);

	/*
	 * We request an aggressive scan if the table's frozen Xid is now older
	 * than or equal to the requested Xid full-table scan limit; or if the
	 * table's minimum MultiXactId is older than or equal to the requested
	 * mxid full-table scan limit; or if DISABLE_PAGE_SKIPPING was specified.
	 */
	aggressive = TransactionIdPrecedesOrEquals(onerel->rd_rel->relfrozenxid,
											   xidFullScanLimit);
	aggressive |= MultiXactIdPrecedesOrEquals(onerel->rd_rel->relminmxid,
											  mxactFullScanLimit);
	if (options & VACOPT_DISABLE_PAGE_SKIPPING)
		aggressive = true;

	vacrelstats = (LVRelStats *) palloc0(sizeof(LVRelStats));

	vacrelstats->old_rel_pages = onerel->rd_rel->relpages;
	vacrelstats->old_rel_tuples = onerel->rd_rel->reltuples;
	vacrelstats->num_index_scans = 0;
	vacrelstats->pages_removed = 0;
	vacrelstats->lock_waiter_detected = false;

	if (parallel_vacuum_workers > -1)
	{
		vacrelstats->nindexes = list_length(RelationGetIndexList(onerel));

		/* Do the parallel vacuum */
		parallel_lazy_scan_heap(onerel, vacrelstats, options, aggressive,
								parallel_vacuum_workers);
	}
	else
	{
		vac_open_indexes(onerel, RowExclusiveLock, &nindexes, &Irel);
		vacrelstats->nindexes = nindexes;

		lazy_scan_heap(vacrelstats, onerel, Irel, nindexes, NULL,
					   options, aggressive);

		/* Done with indexes */
		vac_close_indexes(nindexes, Irel, RowExclusiveLock);
	}

	/*
	 * Compute whether we actually scanned the all unfrozen pages. If we did,
	 * we can adjust relfrozenxid and relminmxid.
	 *
	 * NB: We need to check this before truncating the relation, because that
	 * will change ->rel_pages.
	 */
	if ((vacrelstats->scanned_pages + vacrelstats->frozenskipped_pages)
		< vacrelstats->rel_pages)
	{
		Assert(!aggressive);
		scanned_all_unfrozen = false;
	}
	else
		scanned_all_unfrozen = true;

	/*
	 * Optionally truncate the relation.
	 */
	if (should_attempt_truncation(vacrelstats))
		lazy_truncate_heap(onerel, vacrelstats);

	/* Report that we are now doing final cleanup */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_FINAL_CLEANUP);

	/* Vacuum the Free Space Map */
	FreeSpaceMapVacuum(onerel);

	/*
	 * Update statistics in pg_class.
	 *
	 * A corner case here is that if we scanned no pages at all because every
	 * page is all-visible, we should not update relpages/reltuples, because
	 * we have no new information to contribute.  In particular this keeps us
	 * from replacing relpages=reltuples=0 (which means "unknown tuple
	 * density") with nonzero relpages and reltuples=0 (which means "zero
	 * tuple density") unless there's some actual evidence for the latter.
	 *
	 * We do update relallvisible even in the corner case, since if the table
	 * is all-visible we'd definitely like to know that.  But clamp the value
	 * to be not more than what we're setting relpages to.
	 *
	 * Also, don't change relfrozenxid/relminmxid if we skipped any pages,
	 * since then we don't know for certain that all tuples have a newer xmin.
	 */
	new_rel_pages = vacrelstats->rel_pages;
	new_rel_tuples = vacrelstats->new_rel_tuples;
	if (vacrelstats->scanned_pages == 0 && new_rel_pages > 0)
	{
		new_rel_pages = vacrelstats->old_rel_pages;
		new_rel_tuples = vacrelstats->old_rel_tuples;
	}

	visibilitymap_count(onerel, &new_rel_allvisible, NULL);
	if (new_rel_allvisible > new_rel_pages)
		new_rel_allvisible = new_rel_pages;

	new_frozen_xid = scanned_all_unfrozen ? FreezeLimit : InvalidTransactionId;
	new_min_multi = scanned_all_unfrozen ? MultiXactCutoff : InvalidMultiXactId;

	vac_update_relstats(onerel,
						new_rel_pages,
						new_rel_tuples,
						new_rel_allvisible,
						(vacrelstats->nindexes != 0),
						new_frozen_xid,
						new_min_multi,
						false);

	/* report results to the stats collector, too */
	new_live_tuples = new_rel_tuples - vacrelstats->new_dead_tuples;
	if (new_live_tuples < 0)
		new_live_tuples = 0;	/* just in case */

	pgstat_report_vacuum(RelationGetRelid(onerel),
						 onerel->rd_rel->relisshared,
						 new_live_tuples,
						 vacrelstats->new_dead_tuples);
	pgstat_progress_end_command();

	/* and log the action if appropriate */
	if (IsAutoVacuumWorkerProcess() && params->log_min_duration >= 0)
	{
		TimestampTz endtime = GetCurrentTimestamp();

		if (params->log_min_duration == 0 ||
			TimestampDifferenceExceeds(starttime, endtime,
									   params->log_min_duration))
		{
			StringInfoData buf;

			TimestampDifference(starttime, endtime, &secs, &usecs);

			read_rate = 0;
			write_rate = 0;
			if ((secs > 0) || (usecs > 0))
			{
				read_rate = (double) BLCKSZ *VacuumPageMiss / (1024 * 1024) /
							(secs + usecs / 1000000.0);
				write_rate = (double) BLCKSZ *VacuumPageDirty / (1024 * 1024) /
							(secs + usecs / 1000000.0);
			}

			/*
			 * This is pretty messy, but we split it up so that we can skip
			 * emitting individual parts of the message when not applicable.
			 */
			initStringInfo(&buf);
			appendStringInfo(&buf, _("automatic vacuum of table \"%s.%s.%s\": index scans: %d\n"),
							 get_database_name(MyDatabaseId),
							 get_namespace_name(RelationGetNamespace(onerel)),
							 RelationGetRelationName(onerel),
							 vacrelstats->num_index_scans);
			appendStringInfo(&buf, _("pages: %u removed, %u remain, %u skipped due to pins, %u skipped frozen\n"),
							 vacrelstats->pages_removed,
							 vacrelstats->rel_pages,
							 vacrelstats->pinskipped_pages,
							 vacrelstats->frozenskipped_pages);
			appendStringInfo(&buf,
							 _("tuples: %.0f removed, %.0f remain, %.0f are dead but not yet removable\n"),
							 vacrelstats->tuples_deleted,
							 vacrelstats->new_rel_tuples,
							 vacrelstats->new_dead_tuples);
			appendStringInfo(&buf,
						 _("buffer usage: %d hits, %d misses, %d dirtied\n"),
							 VacuumPageHit,
							 VacuumPageMiss,
							 VacuumPageDirty);
			appendStringInfo(&buf, _("avg read rate: %.3f MB/s, avg write rate: %.3f MB/s\n"),
							 read_rate, write_rate);
			appendStringInfo(&buf, _("system usage: %s"), pg_rusage_show(&ru0));

			ereport(LOG,
					(errmsg_internal("%s", buf.data)));
			pfree(buf.data);
		}
	}
}

/*
 * For Hot Standby we need to know the highest transaction id that will
 * be removed by any change. VACUUM proceeds in a number of passes so
 * we need to consider how each pass operates. The first phase runs
 * heap_page_prune(), which can issue XLOG_HEAP2_CLEAN records as it
 * progresses - these will have a latestRemovedXid on each record.
 * In some cases this removes all of the tuples to be removed, though
 * often we have dead tuples with index pointers so we must remember them
 * for removal in phase 3. Index records for those rows are removed
 * in phase 2 and index blocks do not have MVCC information attached.
 * So before we can allow removal of any index tuples we need to issue
 * a WAL record containing the latestRemovedXid of rows that will be
 * removed in phase three. This allows recovery queries to block at the
 * correct place, i.e. before phase two, rather than during phase three
 * which would be after the rows have become inaccessible.
 */
static void
vacuum_log_cleanup_info(Relation rel, LVRelStats *vacrelstats)
{
	/*
	 * Skip this for relations for which no WAL is to be written, or if we're
	 * not trying to support archive recovery.
	 */
	if (!RelationNeedsWAL(rel) || !XLogIsNeeded())
		return;

	/*
	 * No need to write the record at all unless it contains a valid value
	 */
	if (TransactionIdIsValid(vacrelstats->latestRemovedXid))
		(void) log_heap_cleanup_info(rel->rd_node, vacrelstats->latestRemovedXid);
}

/*
 * Launch parallel vacuum workers specified by wnum and then enter the main
 * logic for arbiter process. After all worker finished, gather the vacuum
 * result of all vacuum workers. In parallel vacuum, All of vacuum workers
 * scan a relation using parallel heap scan description that is stored in
 * DSM tagged by VACUUM_KEY_PARALLEL_SCAN, and each vacuum worker is assigned
 * different indexes.  The vacuum relevant options and some threshoulds,
 * for example, OldestXmin, FreezeLimit and MultiXactCutoff, are stored in DSM
 * tagged by VACUUM_KEY_TASK.  Each worker has its own dead tuple array in
 * DSM tagged by VACUUM_KEY_DEAD_TUPLES. And information related to parallel
 * vacuum is stored in DSM tagged by VACUUM_KEY_PARALLEL_STATE. Updating index
 * statistics according to result of index vaucum can not be done by vacuum
 * worker, so we store such statistics into DSM tagged by VACUUM_KEY_INDEX_STATS
 * once, and the launcher process will update it later. The above six memory
 * spaces in DSM are shared by all of vacuum workers.
 *
 * The vacuum worker assigned to some indexes is responsible for vacuum on
 * index.
 */
static void
parallel_lazy_scan_heap(Relation onerel, LVRelStats *vacrelstats,
						int options, bool aggressive, int wnum)
{
	ParallelContext	*pcxt;
	long maxtuples;
	LVIndStats *vacindstats;

	vacindstats = (LVIndStats *) palloc(sizeof(LVIndStats) * vacrelstats->nindexes);

	EnterParallelMode();

	/* Create parallel context and initialize it */
	pcxt = CreateParallelContext(lazy_vacuum_worker, wnum);

	/* Estimate DSM size for parallel vacuum */
	maxtuples = (int) lazy_get_max_dead_tuple(vacrelstats);
	vacrelstats->max_dead_tuples = maxtuples;
	lazy_estimate_dsm(pcxt, maxtuples, vacrelstats->nindexes);

	fprintf(stderr, "--- maxtuples %ld ---\n", maxtuples);

	/* Initialize DSM for parallel vacuum */
	InitializeParallelDSM(pcxt);
	lazy_initialize_dsm(pcxt, onerel, vacrelstats, options, aggressive);

	/* Launch workers */
	LaunchParallelWorkers(pcxt);

	/* Wait for workers finished vacuum */
	WaitForParallelWorkersToFinish(pcxt);

	/* Gather the result of vacuum statistics from all workers */
	lazy_gather_vacuum_stats(pcxt, vacrelstats, vacindstats);

	/* Now we can compute the new value for pg_class.reltuples */
	vacrelstats->new_rel_tuples = vac_estimate_reltuples(onerel, false,
														 vacrelstats->rel_pages,
														 vacrelstats->scanned_pages,
														 vacrelstats->scanned_tuples);
	DestroyParallelContext(pcxt);
	ExitParallelMode();

	/* After parallel mode, we can update index statistics */
	lazy_update_index_stats(onerel, vacindstats);
}

/*
 * Entry function of parallel vacuum worker.
 */
static void
lazy_vacuum_worker(dsm_segment *seg, shm_toc *toc)
{
	ParallelHeapScanDesc pscan;
	LVRelStats *vacrelstats;
	int options;
	bool aggressive;
	Relation rel;
	Relation *indrel;
	int nindexes_worker;

	fprintf(stderr, " worker %d %d\n", MyProcPid, ParallelWorkerNumber);
	pg_usleep(10 * 1000L * 1000L);

	/* Look up and initialize information and task */
	lazy_initialize_worker(toc, &pscan, &vacrelstats, &options,
							   &aggressive);

	rel = relation_open(pscan->phs_relid, ShareUpdateExclusiveLock);

	/* Open all indexes */
	vac_open_indexes(rel, RowExclusiveLock, &nindexes_worker,
					 &indrel);

	/* Do lazy vacuum */
	lazy_scan_heap(vacrelstats, rel, indrel, vacrelstats->nindexes,
				   pscan, options, aggressive);

	heap_close(rel, ShareUpdateExclusiveLock);
	vac_close_indexes(vacrelstats->nindexes, indrel, RowExclusiveLock);
}

/*
 *	lazy_scan_heap() -- scan an open heap relation
 *
 *		This routine prunes each page in the heap, which will among other
 *		things truncate dead tuples to dead line pointers, defragment the
 *		page, and set commit status bits (see heap_page_prune).  It also uses
 *		lists of dead tuples and pages with free space, calculates statistics
 *		on the number of live tuples in the heap, and marks pages as
 *		all-visible if appropriate.  When done, or when we run low on space for
 *		dead-tuple TIDs, invoke vacuuming of assigned indexes and call lazy_vacuum_heap
 *		to reclaim dead line pointers. In parallel vacuum, we need to synchronize
 *      at where scanning heap finished and vacuuming heap finished. The vacuum
 *      worker reached to that point first need to wait for other vacuum workers
 *      reached to the same point.
 *
 *		This routine scans heap pages using parallel heap scan infrastructure
 *		if pscan is not NULL. Otherwise we use LVScanDesc instead.
 *
 *		If there are no indexes then we can reclaim line pointers on the fly;
 *		dead line pointers need only be retained until all index pointers that
 *		reference them have been killed.
 */
static void
lazy_scan_heap(LVRelStats *vacrelstats, Relation onerel, Relation *Irel,
			   int nindexes, ParallelHeapScanDesc pscan, int options,
			   bool aggressive)
{
	BlockNumber blkno;
	BlockNumber nblocks;
	HeapTupleData tuple;
	LVScanDesc lvscan;
	LVIndStats *vacindstats;
	char	   *relname;
	BlockNumber empty_pages,
				vacuumed_pages;
	double		num_tuples,
				tups_vacuumed,
				nkeep,
				nunused;
	IndexBulkDeleteResult **indstats;
	int			i;
	PGRUsage	ru0;
	Buffer		vmbuffer = InvalidBuffer;
	xl_heap_freeze_tuple *frozen;
	StringInfoData buf;
	bool		all_visible_according_to_vm;

	const int	initprog_index[] = {
		PROGRESS_VACUUM_PHASE,
		PROGRESS_VACUUM_TOTAL_HEAP_BLKS,
		PROGRESS_VACUUM_MAX_DEAD_TUPLES
	};
	int64		initprog_val[3];

	pg_rusage_init(&ru0);

	relname = RelationGetRelationName(onerel);
	ereport(elevel,
			(errmsg("vacuuming \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(onerel)),
					relname)));

	empty_pages = vacuumed_pages = 0;
	num_tuples = tups_vacuumed = nkeep = nunused = 0;
	nblocks = RelationGetNumberOfBlocks(onerel);

	indstats = (IndexBulkDeleteResult **)
		palloc0(sizeof(IndexBulkDeleteResult *) * nindexes);

	vacrelstats->rel_pages = nblocks;
	vacrelstats->scanned_pages = 0;
	vacrelstats->nonempty_pages = 0;
	vacrelstats->latestRemovedXid = InvalidTransactionId;

	lazy_space_alloc(vacrelstats, nblocks);
	frozen = palloc(sizeof(xl_heap_freeze_tuple) * MaxHeapTuplesPerPage);

	/* Array of index vacuum statistics */
	vacindstats = vacrelstats->vacindstats;

	/* Begin heap scan for vacuum */
	lvscan = lv_beginscan(vacrelstats, pscan, onerel);

	/* Report that we're scanning the heap, advertising total # of blocks */
	initprog_val[0] = PROGRESS_VACUUM_PHASE_SCAN_HEAP;
	initprog_val[1] = nblocks;
	initprog_val[2] = vacrelstats->max_dead_tuples;
	pgstat_progress_update_multi_param(3, initprog_index, initprog_val);

	lazy_set_my_vacstate(vacrelstats->pstate, VACSTATE_SCANNING, false, false);

	while((blkno = lazy_scan_heap_get_nextpage(onerel, vacrelstats, lvscan,
											   &all_visible_according_to_vm,
											   &vmbuffer, options, aggressive)) !=
		  InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offnum,
					maxoff;
		bool		tupgone,
					hastup;
		int			prev_dead_count;
		int			nfrozen;
		Size		freespace;
		bool		all_visible;
		bool		all_frozen = true;	/* provided all_visible is also true */
		bool		has_dead_tuples;
		TransactionId visibility_cutoff_xid = InvalidTransactionId;

		pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno);

		vacuum_delay_point();

		/*
		 * If we are close to overrunning the available space for dead-tuple
		 * TIDs, pause and do a cycle of vacuuming before we tackle this page.
		 */
		if ((vacrelstats->max_dead_tuples - MyDeadTuple->n_dt) < MaxHeapTuplesPerPage &&
			MyDeadTuple->n_dt > 0)
		{
			const int	hvp_index[] = {
				PROGRESS_VACUUM_PHASE,
				PROGRESS_VACUUM_NUM_INDEX_VACUUMS
			};
			int64		hvp_val[2];

			/*
			 * Here, scanning heap is done and going to reclaim dead tuples
			 * actually. Because other vacuum worker might not finished yet,
			 * we need to wait for other workers to finish.
			 */
			lazy_set_vacstate_and_wait_prepared(vacrelstats->pstate);
			fprintf(stderr, "[%d] (%d)      SYNCED going to vacum actually\n",
					MyProcPid, ParallelWorkerNumber);

			/*
			 * Before beginning index vacuuming, we release any pin we may
			 * hold on the visibility map page.  This isn't necessary for
			 * correctness, but we do it anyway to avoid holding the pin
			 * across a lengthy, unrelated operation.
			 */
			if (BufferIsValid(vmbuffer))
			{
				ReleaseBuffer(vmbuffer);
				vmbuffer = InvalidBuffer;
			}

			/* Log cleanup info before we touch indexes */
			vacuum_log_cleanup_info(onerel, vacrelstats);

			/* Report that we are now vacuuming indexes */
			pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
										 PROGRESS_VACUUM_PHASE_VACUUM_INDEX);

			/* Remove assidned index entries */
			for (i = 0; i < nindexes; i++)
			{
				if (IsAssignedIndex(i, vacrelstats->pstate->nworkers))
					lazy_vacuum_index(Irel[i], &indstats[i], vacrelstats);
			}

			/*
			 * Report that we are now vacuuming the heap.  We also increase
			 * the number of index scans here; note that by using
			 * pgstat_progress_update_multi_param we can update both
			 * parameters atomically.
			 */
			hvp_val[0] = PROGRESS_VACUUM_PHASE_VACUUM_HEAP;
			hvp_val[1] = vacrelstats->num_index_scans + 1;
			pgstat_progress_update_multi_param(2, hvp_index, hvp_val);

			/* Remove tuples from heap */
			lazy_vacuum_heap(onerel, vacrelstats);


			/* Report that we are once again scanning the heap */
			pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
										 PROGRESS_VACUUM_PHASE_SCAN_HEAP);

			/*
			 * Forget the now-vacuumed tuples, and press on, but be careful
			 * not to reset latestRemovedXid since we want that value to be
			 * valid. In parallel vacuum, we do that later process.
			 */
			if (vacrelstats->pstate == NULL)
				lazy_clear_dead_tuple(vacrelstats);

			/*
			 * Here, we've done to vacuum on heap and going to begin the next
			 * scan on heap. Wait until all vacuum worker to finished vacuum.
			 * Once all vacuum worker finished, all of dead tuple array is cleared
			 * by arbiter process.
			 */
			lazy_set_vacstate_and_wait_finished(vacrelstats);
			fprintf(stderr, "[%d] (%d)      SYNCED going to NEXT SCAN\n",
					MyProcPid, ParallelWorkerNumber);
			vacrelstats->num_index_scans++;
		}

		/*
		 * Pin the visibility map page in case we need to mark the page
		 * all-visible.  In most cases this will be very cheap, because we'll
		 * already have the correct page pinned anyway.  However, it's
		 * possible that (a) next_unskippable_block is covered by a different
		 * VM page than the current block or (b) we released our pin and did a
		 * cycle of index vacuuming.
		 *
		 */
		visibilitymap_pin(onerel, blkno, &vmbuffer);

		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, vac_strategy);

		/* We need buffer cleanup lock so that we can prune HOT chains. */
		if (!ConditionalLockBufferForCleanup(buf))
		{
			/*
			 * If we're not performing an aggressive scan to guard against XID
			 * wraparound, and we don't want to forcibly check the page, then
			 * it's OK to skip vacuuming pages we get a lock conflict on. They
			 * will be dealt with in some future vacuum.
			 */
			if (!aggressive && !FORCE_CHECK_PAGE(blkno))
			{
				ReleaseBuffer(buf);
				vacrelstats->pinskipped_pages++;
				continue;
			}

			/*
			 * Read the page with share lock to see if any xids on it need to
			 * be frozen.  If not we just skip the page, after updating our
			 * scan statistics.  If there are some, we wait for cleanup lock.
			 *
			 * We could defer the lock request further by remembering the page
			 * and coming back to it later, or we could even register
			 * ourselves for multiple buffers and then service whichever one
			 * is received first.  For now, this seems good enough.
			 *
			 * If we get here with aggressive false, then we're just forcibly
			 * checking the page, and so we don't want to insist on getting
			 * the lock; we only need to know if the page contains tuples, so
			 * that we can update nonempty_pages correctly.  It's convenient
			 * to use lazy_check_needs_freeze() for both situations, though.
			 */
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			if (!lazy_check_needs_freeze(buf, &hastup))
			{
				UnlockReleaseBuffer(buf);
				vacrelstats->scanned_pages++;
				vacrelstats->pinskipped_pages++;
				if (hastup)
					vacrelstats->nonempty_pages = blkno + 1;
				continue;
			}
			if (!aggressive)
			{
				/*
				 * Here, we must not advance scanned_pages; that would amount
				 * to claiming that the page contains no freezable tuples.
				 */
				UnlockReleaseBuffer(buf);
				vacrelstats->pinskipped_pages++;
				if (hastup)
					vacrelstats->nonempty_pages = blkno + 1;
				continue;
			}
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockBufferForCleanup(buf);
			/* drop through to normal processing */
		}

		vacrelstats->scanned_pages++;

		page = BufferGetPage(buf);

		if (PageIsNew(page))
		{
			/*
			 * An all-zeroes page could be left over if a backend extends the
			 * relation but crashes before initializing the page. Reclaim such
			 * pages for use.
			 *
			 * We have to be careful here because we could be looking at a
			 * page that someone has just added to the relation and not yet
			 * been able to initialize (see RelationGetBufferForTuple). To
			 * protect against that, release the buffer lock, grab the
			 * relation extension lock momentarily, and re-lock the buffer. If
			 * the page is still uninitialized by then, it must be left over
			 * from a crashed backend, and we can initialize it.
			 *
			 * We don't really need the relation lock when this is a new or
			 * temp relation, but it's probably not worth the code space to
			 * check that, since this surely isn't a critical path.
			 *
			 * Note: the comparable code in vacuum.c need not worry because
			 * it's got exclusive lock on the whole relation.
			 */
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockRelationForExtension(onerel, ExclusiveLock);
			UnlockRelationForExtension(onerel, ExclusiveLock);
			LockBufferForCleanup(buf);
			if (PageIsNew(page))
			{
				ereport(WARNING,
				(errmsg("relation \"%s\" page %u is uninitialized --- fixing",
						relname, blkno)));
				PageInit(page, BufferGetPageSize(buf), 0);
				empty_pages++;
			}
			freespace = PageGetHeapFreeSpace(page);
			MarkBufferDirty(buf);
			UnlockReleaseBuffer(buf);

			RecordPageWithFreeSpace(onerel, blkno, freespace);
			continue;
		}

		if (PageIsEmpty(page))
		{
			empty_pages++;
			freespace = PageGetHeapFreeSpace(page);

			/* empty pages are always all-visible and all-frozen */
			if (!PageIsAllVisible(page))
			{
				START_CRIT_SECTION();

				/* mark buffer dirty before writing a WAL record */
				MarkBufferDirty(buf);

				/*
				 * It's possible that another backend has extended the heap,
				 * initialized the page, and then failed to WAL-log the page
				 * due to an ERROR.  Since heap extension is not WAL-logged,
				 * recovery might try to replay our record setting the page
				 * all-visible and find that the page isn't initialized, which
				 * will cause a PANIC.  To prevent that, check whether the
				 * page has been previously WAL-logged, and if not, do that
				 * now.
				 */
				if (RelationNeedsWAL(onerel) &&
					PageGetLSN(page) == InvalidXLogRecPtr)
					log_newpage_buffer(buf, true);

				PageSetAllVisible(page);
				visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
								  vmbuffer, InvalidTransactionId,
					   VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN);
				END_CRIT_SECTION();
			}

			UnlockReleaseBuffer(buf);
			RecordPageWithFreeSpace(onerel, blkno, freespace);
			continue;
		}

		/*
		 * Prune all HOT-update chains in this page.
		 *
		 * We count tuples removed by the pruning step as removed by VACUUM.
		 */
		tups_vacuumed += heap_page_prune(onerel, buf, OldestXmin, false,
										 &vacrelstats->latestRemovedXid);

		/*
		 * Now scan the page to collect vacuumable items and check for tuples
		 * requiring freezing.
		 */
		all_visible = true;
		has_dead_tuples = false;
		nfrozen = 0;
		hastup = false;
		prev_dead_count = MyDeadTuple->n_dt;
		maxoff = PageGetMaxOffsetNumber(page);

		/*
		 * Note: If you change anything in the loop below, also look at
		 * heap_page_is_all_visible to see if that needs to be changed.
		 */
		for (offnum = FirstOffsetNumber;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid;

			itemid = PageGetItemId(page, offnum);

			/* Unused items require no processing, but we count 'em */
			if (!ItemIdIsUsed(itemid))
			{
				nunused += 1;
				continue;
			}

			/* Redirect items mustn't be touched */
			if (ItemIdIsRedirected(itemid))
			{
				hastup = true;	/* this page won't be truncatable */
				continue;
			}

			ItemPointerSet(&(tuple.t_self), blkno, offnum);

			/*
			 * DEAD item pointers are to be vacuumed normally; but we don't
			 * count them in tups_vacuumed, else we'd be double-counting (at
			 * least in the common case where heap_page_prune() just freed up
			 * a non-HOT tuple).
			 */
			if (ItemIdIsDead(itemid))
			{
				lazy_record_dead_tuple(vacrelstats, &(tuple.t_self));
				all_visible = false;
				continue;
			}

			Assert(ItemIdIsNormal(itemid));

			tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(onerel);

			tupgone = false;

			switch (HeapTupleSatisfiesVacuum(&tuple, OldestXmin, buf))
			{
				case HEAPTUPLE_DEAD:

					/*
					 * Ordinarily, DEAD tuples would have been removed by
					 * heap_page_prune(), but it's possible that the tuple
					 * state changed since heap_page_prune() looked.  In
					 * particular an INSERT_IN_PROGRESS tuple could have
					 * changed to DEAD if the inserter aborted.  So this
					 * cannot be considered an error condition.
					 *
					 * If the tuple is HOT-updated then it must only be
					 * removed by a prune operation; so we keep it just as if
					 * it were RECENTLY_DEAD.  Also, if it's a heap-only
					 * tuple, we choose to keep it, because it'll be a lot
					 * cheaper to get rid of it in the next pruning pass than
					 * to treat it like an indexed tuple.
					 */
					if (HeapTupleIsHotUpdated(&tuple) ||
						HeapTupleIsHeapOnly(&tuple))
						nkeep += 1;
					else
						tupgone = true; /* we can delete the tuple */
					all_visible = false;
					break;
				case HEAPTUPLE_LIVE:
					/* Tuple is good --- but let's do some validity checks */
					if (onerel->rd_rel->relhasoids &&
						!OidIsValid(HeapTupleGetOid(&tuple)))
						elog(WARNING, "relation \"%s\" TID %u/%u: OID is invalid",
							 relname, blkno, offnum);

					/*
					 * Is the tuple definitely visible to all transactions?
					 *
					 * NB: Like with per-tuple hint bits, we can't set the
					 * PD_ALL_VISIBLE flag if the inserter committed
					 * asynchronously. See SetHintBits for more info. Check
					 * that the tuple is hinted xmin-committed because of
					 * that.
					 */
					if (all_visible)
					{
						TransactionId xmin;

						if (!HeapTupleHeaderXminCommitted(tuple.t_data))
						{
							all_visible = false;
							break;
						}

						/*
						 * The inserter definitely committed. But is it old
						 * enough that everyone sees it as committed?
						 */
						xmin = HeapTupleHeaderGetXmin(tuple.t_data);
						if (!TransactionIdPrecedes(xmin, OldestXmin))
						{
							all_visible = false;
							break;
						}

						/* Track newest xmin on page. */
						if (TransactionIdFollows(xmin, visibility_cutoff_xid))
							visibility_cutoff_xid = xmin;
					}
					break;
				case HEAPTUPLE_RECENTLY_DEAD:

					/*
					 * If tuple is recently deleted then we must not remove it
					 * from relation.
					 */
					nkeep += 1;
					all_visible = false;
					break;
				case HEAPTUPLE_INSERT_IN_PROGRESS:
					/* This is an expected case during concurrent vacuum */
					all_visible = false;
					break;
				case HEAPTUPLE_DELETE_IN_PROGRESS:
					/* This is an expected case during concurrent vacuum */
					all_visible = false;
					break;
				default:
					elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
					break;
			}

			if (tupgone)
			{
				lazy_record_dead_tuple(vacrelstats, &(tuple.t_self));
				HeapTupleHeaderAdvanceLatestRemovedXid(tuple.t_data,
											 &vacrelstats->latestRemovedXid);
				tups_vacuumed += 1;
				has_dead_tuples = true;
			}
			else
			{
				bool		tuple_totally_frozen;

				num_tuples += 1;
				hastup = true;

				/*
				 * Each non-removable tuple must be checked to see if it needs
				 * freezing.  Note we already have exclusive buffer lock.
				 */
				if (heap_prepare_freeze_tuple(tuple.t_data, FreezeLimit,
										   MultiXactCutoff, &frozen[nfrozen],
											  &tuple_totally_frozen))
					frozen[nfrozen++].offset = offnum;

				if (!tuple_totally_frozen)
					all_frozen = false;
			}
		}						/* scan along page */

		/*
		 * If we froze any tuples, mark the buffer dirty, and write a WAL
		 * record recording the changes.  We must log the changes to be
		 * crash-safe against future truncation of CLOG.
		 */
		if (nfrozen > 0)
		{
			START_CRIT_SECTION();

			MarkBufferDirty(buf);

			/* execute collected freezes */
			for (i = 0; i < nfrozen; i++)
			{
				ItemId		itemid;
				HeapTupleHeader htup;

				itemid = PageGetItemId(page, frozen[i].offset);
				htup = (HeapTupleHeader) PageGetItem(page, itemid);

				heap_execute_freeze_tuple(htup, &frozen[i]);
			}

			/* Now WAL-log freezing if necessary */
			if (RelationNeedsWAL(onerel))
			{
				XLogRecPtr	recptr;

				recptr = log_heap_freeze(onerel, buf, FreezeLimit,
										 frozen, nfrozen);
				PageSetLSN(page, recptr);
			}

			END_CRIT_SECTION();
		}

		/*
		 * If there are no indexes then we can vacuum the page right now
		 * instead of doing a second scan. Because each parallel worker uses its
		 * own dead tuple area they can vacuum independently.
		 */
		if (Irel == NULL && MyDeadTuple->n_dt > 0)
		{
			/* Remove tuples from heap */
			lazy_vacuum_page(onerel, blkno, buf, 0, vacrelstats, &vmbuffer);
			has_dead_tuples = false;

			/*
			 * Forget the now-vacuumed tuples, and press on, but be careful
			 * not to reset latestRemovedXid since we want that value to be
			 * valid.
			 */
			lazy_clear_dead_tuple(vacrelstats);
			vacuumed_pages++;
		}

		freespace = PageGetHeapFreeSpace(page);

		/* mark page all-visible, if appropriate */
		if (all_visible && !all_visible_according_to_vm)
		{
			uint8		flags = VISIBILITYMAP_ALL_VISIBLE;

			if (all_frozen)
				flags |= VISIBILITYMAP_ALL_FROZEN;

			/*
			 * It should never be the case that the visibility map page is set
			 * while the page-level bit is clear, but the reverse is allowed
			 * (if checksums are not enabled).  Regardless, set the both bits
			 * so that we get back in sync.
			 *
			 * NB: If the heap page is all-visible but the VM bit is not set,
			 * we don't need to dirty the heap page.  However, if checksums
			 * are enabled, we do need to make sure that the heap page is
			 * dirtied before passing it to visibilitymap_set(), because it
			 * may be logged.  Given that this situation should only happen in
			 * rare cases after a crash, it is not worth optimizing.
			 */
			PageSetAllVisible(page);
			MarkBufferDirty(buf);
			visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
							  vmbuffer, visibility_cutoff_xid, flags);
		}

		/*
		 * As of PostgreSQL 9.2, the visibility map bit should never be set if
		 * the page-level bit is clear.  However, it's possible that the bit
		 * got cleared after we checked it and before we took the buffer
		 * content lock, so we must recheck before jumping to the conclusion
		 * that something bad has happened.
		 */
		else if (all_visible_according_to_vm && !PageIsAllVisible(page)
				 && VM_ALL_VISIBLE(onerel, blkno, &vmbuffer))
		{
			elog(WARNING, "page is not marked all-visible but visibility map bit is set in relation \"%s\" page %u",
				 relname, blkno);
			visibilitymap_clear(onerel, blkno, vmbuffer,
								VISIBILITYMAP_VALID_BITS);
		}

		/*
		 * It's possible for the value returned by GetOldestXmin() to move
		 * backwards, so it's not wrong for us to see tuples that appear to
		 * not be visible to everyone yet, while PD_ALL_VISIBLE is already
		 * set. The real safe xmin value never moves backwards, but
		 * GetOldestXmin() is conservative and sometimes returns a value
		 * that's unnecessarily small, so if we see that contradiction it just
		 * means that the tuples that we think are not visible to everyone yet
		 * actually are, and the PD_ALL_VISIBLE flag is correct.
		 *
		 * There should never be dead tuples on a page with PD_ALL_VISIBLE
		 * set, however.
		 */
		else if (PageIsAllVisible(page) && has_dead_tuples)
		{
			elog(WARNING, "page containing dead tuples is marked as all-visible in relation \"%s\" page %u",
				 relname, blkno);
			PageClearAllVisible(page);
			MarkBufferDirty(buf);
			visibilitymap_clear(onerel, blkno, vmbuffer,
								VISIBILITYMAP_VALID_BITS);
		}

		/*
		 * If the all-visible page is turned out to be all-frozen but not
		 * marked, we should so mark it.  Note that all_frozen is only valid
		 * if all_visible is true, so we must check both.
		 */
		else if (all_visible_according_to_vm && all_visible && all_frozen &&
				 !VM_ALL_FROZEN(onerel, blkno, &vmbuffer))
		{
			/*
			 * We can pass InvalidTransactionId as the cutoff XID here,
			 * because setting the all-frozen bit doesn't cause recovery
			 * conflicts.
			 */
			visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr,
							  vmbuffer, InvalidTransactionId,
							  VISIBILITYMAP_ALL_FROZEN);
		}

		UnlockReleaseBuffer(buf);

		/* Remember the location of the last page with nonremovable tuples */
		if (hastup)
			vacrelstats->nonempty_pages = blkno + 1;

		/*
		 * If we remembered any tuples for deletion, then the page will be
		 * visited again by lazy_vacuum_heap, which will compute and record
		 * its post-compaction free space.  If not, then we're done with this
		 * page, so remember its free space as-is.  (This path will always be
		 * taken if there are no indexes.)
		 */
		if (MyDeadTuple->n_dt == prev_dead_count)
			RecordPageWithFreeSpace(onerel, blkno, freespace);
	}

	/* report that everything is scanned and vacuumed */
	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno);

	pfree(frozen);

	/* save stats for use later */
	vacrelstats->scanned_tuples = num_tuples;
	vacrelstats->tuples_deleted = tups_vacuumed;
	vacrelstats->new_dead_tuples = nkeep;

	/* now we can compute the new value for pg_class.reltuples */
	if (vacrelstats->pstate == NULL)
		vacrelstats->new_rel_tuples = vac_estimate_reltuples(onerel, false,
															 nblocks,
															 vacrelstats->scanned_pages,
															 num_tuples);

	/*
	 * Release any remaining pin on visibility map page.
	 */
	if (BufferIsValid(vmbuffer))
	{
		ReleaseBuffer(vmbuffer);
		vmbuffer = InvalidBuffer;
	}

	/* If any tuples need to be deleted, perform final vacuum cycle */
	/* XXX put a threshold on min number of tuples here? */
	if (MyDeadTuple->n_dt > 0)
	{
		const int	hvp_index[] = {
			PROGRESS_VACUUM_PHASE,
			PROGRESS_VACUUM_NUM_INDEX_VACUUMS
		};
		int64		hvp_val[2];

		/*
		 * Here, scanning heap is done and going to reclaim dead tuples
		 * actually. Because other vacuum worker might not finished yet,
		 * we need to wait for other workers to finish.
		 */
		fprintf(stderr, "[%d] (%d) LAST VACUUM PREPARED\n",
			   MyProcPid, ParallelWorkerNumber);
		lazy_set_vacstate_and_wait_prepared(vacrelstats->pstate);

		/* Log cleanup info before we touch indexes */
		vacuum_log_cleanup_info(onerel, vacrelstats);

		/* Report that we are now vacuuming indexes */
		pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
									 PROGRESS_VACUUM_PHASE_VACUUM_INDEX);

		/* Remove index entries */
		for (i = 0; i < nindexes; i++)
		{
			if (IsAssignedIndex(i, vacrelstats->pstate->nworkers))
				lazy_vacuum_index(Irel[i], &indstats[i], vacrelstats);
		}

		/* Report that we are now vacuuming the heap */
		hvp_val[0] = PROGRESS_VACUUM_PHASE_VACUUM_HEAP;
		hvp_val[1] = vacrelstats->num_index_scans + 1;
		pgstat_progress_update_multi_param(2, hvp_index, hvp_val);

		/* Remove tuples from heap */
		pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
									 PROGRESS_VACUUM_PHASE_VACUUM_HEAP);

		lazy_vacuum_heap(onerel, vacrelstats);

		/*
		 * Here, we've done to vacuum on heap and going to begin the next
		 * scan on heap. Wait until all vacuum worker to finished vacuum.
		 * Once all vacuum worker finished, all of dead tuple array is cleared
		 * by arbiter process.
		 */
		lazy_set_vacstate_and_wait_finished(vacrelstats);
		vacrelstats->num_index_scans++;
	}

	/* Change my vacstate to Complete */
	lazy_set_my_vacstate(vacrelstats->pstate, VACSTATE_COMPLETE, false, true);

	/* report all blocks vacuumed; and that we're cleaning up */
	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno);
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_INDEX_CLEANUP);

	/* Do post-vacuum cleanup and statistics update for each index */
	for (i = 0; i < nindexes; i++)
	{
		if (IsAssignedIndex(i, vacrelstats->pstate->nworkers))
			lazy_cleanup_index(Irel[i], indstats[i], vacrelstats, &vacindstats[i]);
	}

	/* If no indexes, make log report that lazy_vacuum_heap would've made */
	if (vacuumed_pages)
		ereport(elevel,
				(errmsg("\"%s\": removed %.0f row versions in %u pages",
						RelationGetRelationName(onerel),
						tups_vacuumed, vacuumed_pages)));

	lv_endscan(lvscan);

	/* @@@ for debug */
	ereport(LOG,
			(errmsg("(%d) scanned pages %u, scanned tuples %0.f, vacuumed page %u, vacuumed_tuples %0.f, new dead tuple %0.f, num index scan %d",
					ParallelWorkerNumber,
					vacrelstats->scanned_pages,
					vacrelstats->scanned_tuples,
					vacuumed_pages,
					vacrelstats->tuples_deleted,
					vacrelstats->new_dead_tuples,
					vacrelstats->num_index_scans)));

	/*
	 * This is pretty messy, but we split it up so that we can skip emitting
	 * individual parts of the message when not applicable.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 _("%.0f dead row versions cannot be removed yet.\n"),
					 nkeep);
	appendStringInfo(&buf, _("There were %.0f unused item pointers.\n"),
					 nunused);
	appendStringInfo(&buf, ngettext("Skipped %u page due to buffer pins.\n",
									"Skipped %u pages due to buffer pins.\n",
									vacrelstats->pinskipped_pages),
					 vacrelstats->pinskipped_pages);
	appendStringInfo(&buf, ngettext("%u page is entirely empty.\n",
									"%u pages are entirely empty.\n",
									empty_pages),
					 empty_pages);
	appendStringInfo(&buf, _("%s."),
					 pg_rusage_show(&ru0));

	ereport(elevel,
			(errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u out of %u pages",
					RelationGetRelationName(onerel),
					tups_vacuumed, num_tuples,
					vacrelstats->scanned_pages, nblocks),
			 errdetail_internal("%s", buf.data)));
	pfree(buf.data);
}

/*
 * gather_vacuum_stats() -- Gather vacuum statistics from workers
 */
static void
lazy_gather_vacuum_stats(ParallelContext *pcxt, LVRelStats *vacrelstats,
									LVIndStats *vacindstats)
{
	int	i;
	LVRelStats *lvstats_list;
	LVIndStats *lvindstats_list;
	LVRelStats *ws;

	lvstats_list = (LVRelStats *) shm_toc_lookup(pcxt->toc, VACUUM_KEY_VACUUM_STATS);
	lvindstats_list = (LVIndStats *) shm_toc_lookup(pcxt->toc, VACUUM_KEY_INDEX_STATS);

	/* Gather each worker stats */
	for (i = 0; i < pcxt->nworkers; i++)
	{
		LVRelStats *wstats = lvstats_list + sizeof(LVRelStats) * i;

		vacrelstats->scanned_pages += wstats->scanned_pages;
		vacrelstats->pinskipped_pages += wstats->pinskipped_pages;
		vacrelstats->frozenskipped_pages += wstats->frozenskipped_pages;
		vacrelstats->scanned_tuples += wstats->scanned_tuples;
		vacrelstats->new_dead_tuples += wstats->new_dead_tuples;
		vacrelstats->pages_removed += wstats->pages_removed;
		vacrelstats->tuples_deleted += wstats->tuples_deleted;
		vacrelstats->nonempty_pages += wstats->nonempty_pages;
	}

	ws = lvstats_list + sizeof(LVRelStats);
	vacrelstats->rel_pages = ws->rel_pages;
	
	/* Copy index vacuum statistics on DSM to local memory */
	memcpy(vacindstats, lvindstats_list, sizeof(LVIndStats) * vacrelstats->nindexes);
}

/*
 * lazy_update_index_stats() -- Update index vacuum statistics
 *
 * This routine can not be called in parallel context.
 */
static void
lazy_update_index_stats(Relation onerel, LVIndStats *vacindstats)
{
	List *indexoidlist;
	ListCell *indexoidscan;
	int i;

	indexoidlist = RelationGetIndexList(onerel);
	i = 0;

	foreach(indexoidscan, indexoidlist)
	{
		Oid indexoid = lfirst_oid(indexoidscan);
		Relation indrel;

		/* Update index relation statistics if needed */
		if (vacindstats[i].do_update)
		{
			indrel = index_open(indexoid, RowExclusiveLock);
			vac_update_relstats(indrel,
								vacindstats[i].rel_pages,
								vacindstats[i].rel_tuples,
								0,
								false,
								InvalidTransactionId,
								InvalidMultiXactId,
								false);
			index_close(indrel, RowExclusiveLock);
		}
		i++;
	}

	list_free(indexoidlist);
}

/*
 *	lazy_vacuum_heap() -- second pass over the heap
 *
 *		This routine marks dead tuples as unused and compacts out free
 *		space on their pages.  Pages not having dead tuples recorded from
 *		lazy_scan_heap are not visited at all.
 *
 * Note: the reason for doing this as a second pass is we cannot remove
 * the tuples until we've removed their index entries, and we want to
 * process index entry removal in batches as large as possible.
 */
static void
lazy_vacuum_heap(Relation onerel, LVRelStats *vacrelstats)
{
	int			tupindex;
	int			npages;
	PGRUsage	ru0;
	Buffer		vmbuffer = InvalidBuffer;

	pg_rusage_init(&ru0);
	npages = 0;

	tupindex = 0;

	fprintf(stderr, "[%d] (%d) lazy_vacuum_heap : deadtuples %d\n",
			MyProcPid,
			ParallelWorkerNumber,
			MyDeadTuple->n_dt);

	while (tupindex < MyDeadTuple->n_dt)
	{
		BlockNumber tblk;
		Buffer		buf;
		Page		page;
		Size		freespace;

		vacuum_delay_point();

		tblk = ItemPointerGetBlockNumber(&MyDeadTuple->dt_array[tupindex]);
		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, tblk, RBM_NORMAL,
								 vac_strategy);
		if (!ConditionalLockBufferForCleanup(buf))
		{
			ReleaseBuffer(buf);
			++tupindex;
			continue;
		}
		tupindex = lazy_vacuum_page(onerel, tblk, buf, tupindex, vacrelstats,
									&vmbuffer);

		/* Now that we've compacted the page, record its available space */
		page = BufferGetPage(buf);
		freespace = PageGetHeapFreeSpace(page);

		UnlockReleaseBuffer(buf);
		RecordPageWithFreeSpace(onerel, tblk, freespace);
		npages++;
	}

	if (BufferIsValid(vmbuffer))
	{
		ReleaseBuffer(vmbuffer);
		vmbuffer = InvalidBuffer;
	}

	ereport(elevel,
			(errmsg("\"%s\": removed %d row versions in %d pages",
					RelationGetRelationName(onerel),
					tupindex, npages),
			 errdetail("%s.",
					   pg_rusage_show(&ru0))));
}

/*
 *	lazy_vacuum_page() -- free dead tuples on a page
 *					 and repair its fragmentation.
 *
 * Caller must hold pin and buffer cleanup lock on the buffer.
 *
 * tupindex is the index in MyDeadTuple->dt_array of the first dead
 * tuple for this page.  We assume the rest follow sequentially.
 * The return value is the first tupindex after the tuples of this page.
 */
static int
lazy_vacuum_page(Relation onerel, BlockNumber blkno, Buffer buffer,
				 int tupindex, LVRelStats *vacrelstats,
				 Buffer *vmbuffer)
{
	Page		page = BufferGetPage(buffer);
	OffsetNumber unused[MaxOffsetNumber];
	int			uncnt = 0;
	TransactionId visibility_cutoff_xid;
	bool		all_frozen;

	pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno);

	START_CRIT_SECTION();

	for (; tupindex < MyDeadTuple->n_dt; tupindex++)
	{
		BlockNumber tblk;
		OffsetNumber toff;
		ItemId		itemid;

		tblk = ItemPointerGetBlockNumber(&MyDeadTuple->dt_array[tupindex]);
		if (tblk != blkno)
			break;				/* past end of tuples for this block */
		toff = ItemPointerGetOffsetNumber(&MyDeadTuple->dt_array[tupindex]);
		itemid = PageGetItemId(page, toff);
		ItemIdSetUnused(itemid);
		unused[uncnt++] = toff;
	}

	PageRepairFragmentation(page);

	/*
	 * Mark buffer dirty before we write WAL.
	 */
	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (RelationNeedsWAL(onerel))
	{
		XLogRecPtr	recptr;

		recptr = log_heap_clean(onerel, buffer,
								NULL, 0, NULL, 0,
								unused, uncnt,
								vacrelstats->latestRemovedXid);
		PageSetLSN(page, recptr);
	}

	/*
	 * End critical section, so we safely can do visibility tests (which
	 * possibly need to perform IO and allocate memory!). If we crash now the
	 * page (including the corresponding vm bit) might not be marked all
	 * visible, but that's fine. A later vacuum will fix that.
	 */
	END_CRIT_SECTION();

	/*
	 * Now that we have removed the dead tuples from the page, once again
	 * check if the page has become all-visible.  The page is already marked
	 * dirty, exclusively locked, and, if needed, a full page image has been
	 * emitted in the log_heap_clean() above.
	 */
	if (heap_page_is_all_visible(onerel, buffer, &visibility_cutoff_xid,
								 &all_frozen))
		PageSetAllVisible(page);

	/*
	 * All the changes to the heap page have been done. If the all-visible
	 * flag is now set, also set the VM all-visible bit (and, if possible, the
	 * all-frozen bit) unless this has already been done previously.
	 */
	if (PageIsAllVisible(page))
	{
		uint8		vm_status = visibilitymap_get_status(onerel, blkno, vmbuffer);
		uint8		flags = 0;

		/* Set the VM all-frozen bit to flag, if needed */
		if ((vm_status & VISIBILITYMAP_ALL_VISIBLE) == 0)
			flags |= VISIBILITYMAP_ALL_VISIBLE;
		if ((vm_status & VISIBILITYMAP_ALL_FROZEN) == 0 && all_frozen)
			flags |= VISIBILITYMAP_ALL_FROZEN;

		Assert(BufferIsValid(*vmbuffer));
		if (flags != 0)
			visibilitymap_set(onerel, blkno, buffer, InvalidXLogRecPtr,
							  *vmbuffer, visibility_cutoff_xid, flags);
	}

	return tupindex;
}

/*
 *	lazy_check_needs_freeze() -- scan page to see if any tuples
 *					 need to be cleaned to avoid wraparound
 *
 * Returns true if the page needs to be vacuumed using cleanup lock.
 * Also returns a flag indicating whether page contains any tuples at all.
 */
static bool
lazy_check_needs_freeze(Buffer buf, bool *hastup)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber offnum,
				maxoff;
	HeapTupleHeader tupleheader;

	*hastup = false;

	/* If we hit an uninitialized page, we want to force vacuuming it. */
	if (PageIsNew(page))
		return true;

	/* Quick out for ordinary empty page. */
	if (PageIsEmpty(page))
		return false;

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;

		itemid = PageGetItemId(page, offnum);

		/* this should match hastup test in count_nondeletable_pages() */
		if (ItemIdIsUsed(itemid))
			*hastup = true;

		/* dead and redirect items never need freezing */
		if (!ItemIdIsNormal(itemid))
			continue;

		tupleheader = (HeapTupleHeader) PageGetItem(page, itemid);

		if (heap_tuple_needs_freeze(tupleheader, FreezeLimit,
									MultiXactCutoff, buf))
			return true;
	}							/* scan along page */

	return false;
}


/*
 *	lazy_vacuum_index() -- vacuum one index relation.
 *
 *		Delete all the index entries pointing to tuples listed in
 *		MyDeadTuple->dt_array, and update running statistics.
 */
static void
lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
				  LVRelStats *vacrelstats)
{
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = true;
	ivinfo.message_level = elevel;
	ivinfo.num_heap_tuples = vacrelstats->old_rel_tuples;
	ivinfo.strategy = vac_strategy;

	/* Do bulk deletion */
	*stats = index_bulk_delete(&ivinfo, *stats,
							   lazy_tid_reaped, (void *) vacrelstats);

	fprintf(stderr, "[%d] (%d) lazy_vacuum_index : remoted %d\n",
			MyProcPid, ParallelWorkerNumber, MyDeadTuple->n_dt);
	ereport(elevel,
			(errmsg("scanned index \"%s\" to remove %d row versions",
					RelationGetRelationName(indrel),
					MyDeadTuple->n_dt),
			 errdetail("%s.", pg_rusage_show(&ru0))));
}

/*
 *	lazy_cleanup_index() -- do post-vacuum cleanup for one index relation.
 */
static void
lazy_cleanup_index(Relation indrel,
				   IndexBulkDeleteResult *stats,
				   LVRelStats *vacrelstats,
				   LVIndStats *vacindstats)
{
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = (vacrelstats->scanned_pages < vacrelstats->rel_pages);
	ivinfo.message_level = elevel;
	ivinfo.num_heap_tuples = vacrelstats->new_rel_tuples;
	ivinfo.strategy = vac_strategy;

	stats = index_vacuum_cleanup(&ivinfo, stats);

	if (!stats)
		return;

	/*
	 * Now update statistics in pg_class, but only if the index says the count
	 * is accurate.
	 * In parallel lazy vacuum, the worker can not update these information by
	 * itself, so save to DSM and then the launcher process will update it later.
	 */
	if (!stats->estimated_count)
	{
		if (IsParallelWorker())
		{
			/* Save to shared memory */
			vacindstats->do_update = true;
			vacindstats->rel_pages = stats->num_pages;
			vacindstats->rel_tuples = stats->num_index_tuples;
		}
		else
		{
			vac_update_relstats(indrel,
								stats->num_pages,
								stats->num_index_tuples,
								0,
								false,
								InvalidTransactionId,
								InvalidMultiXactId,
								false);
		}
	}

	ereport(elevel,
			(errmsg("index \"%s\" now contains %.0f row versions in %u pages",
					RelationGetRelationName(indrel),
					stats->num_index_tuples,
					stats->num_pages),
			 errdetail("%.0f index row versions were removed.\n"
			 "%u index pages have been deleted, %u are currently reusable.\n"
					   "%s.",
					   stats->tuples_removed,
					   stats->pages_deleted, stats->pages_free,
					   pg_rusage_show(&ru0))));

	pfree(stats);
}

/*
 * should_attempt_truncation - should we attempt to truncate the heap?
 *
 * Don't even think about it unless we have a shot at releasing a goodly
 * number of pages.  Otherwise, the time taken isn't worth it.
 *
 * Also don't attempt it if we are doing early pruning/vacuuming, because a
 * scan which cannot find a truncated heap page cannot determine that the
 * snapshot is too old to read that page.  We might be able to get away with
 * truncating all except one of the pages, setting its LSN to (at least) the
 * maximum of the truncated range if we also treated an index leaf tuple
 * pointing to a missing heap page as something to trigger the "snapshot too
 * old" error, but that seems fragile and seems like it deserves its own patch
 * if we consider it.
 *
 * This is split out so that we can test whether truncation is going to be
 * called for before we actually do it.  If you change the logic here, be
 * careful to depend only on fields that lazy_scan_heap updates on-the-fly.
 */
static bool
should_attempt_truncation(LVRelStats *vacrelstats)
{
	BlockNumber possibly_freeable;

	possibly_freeable = vacrelstats->rel_pages - vacrelstats->nonempty_pages;
	if (possibly_freeable > 0 &&
		(possibly_freeable >= REL_TRUNCATE_MINIMUM ||
	  possibly_freeable >= vacrelstats->rel_pages / REL_TRUNCATE_FRACTION) &&
		old_snapshot_threshold < 0)
		return true;
	else
		return false;
}

/*
 * lazy_truncate_heap - try to truncate off any empty pages at the end
 */
static void
lazy_truncate_heap(Relation onerel, LVRelStats *vacrelstats)
{
	BlockNumber old_rel_pages = vacrelstats->rel_pages;
	BlockNumber new_rel_pages;
	PGRUsage	ru0;
	int			lock_retry;

	pg_rusage_init(&ru0);

	/* Report that we are now truncating */
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_TRUNCATE);

	/*
	 * Loop until no more truncating can be done.
	 */
	do
	{
		/*
		 * We need full exclusive lock on the relation in order to do
		 * truncation. If we can't get it, give up rather than waiting --- we
		 * don't want to block other backends, and we don't want to deadlock
		 * (which is quite possible considering we already hold a lower-grade
		 * lock).
		 */
		vacrelstats->lock_waiter_detected = false;
		lock_retry = 0;
		while (true)
		{
			if (ConditionalLockRelation(onerel, AccessExclusiveLock))
				break;

			/*
			 * Check for interrupts while trying to (re-)acquire the exclusive
			 * lock.
			 */
			CHECK_FOR_INTERRUPTS();

			if (++lock_retry > (VACUUM_TRUNCATE_LOCK_TIMEOUT /
								VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL))
			{
				/*
				 * We failed to establish the lock in the specified number of
				 * retries. This means we give up truncating.
				 */
				vacrelstats->lock_waiter_detected = true;
				ereport(elevel,
						(errmsg("\"%s\": stopping truncate due to conflicting lock request",
								RelationGetRelationName(onerel))));
				return;
			}

			pg_usleep(VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL * 1000L);
		}

		/*
		 * Now that we have exclusive lock, look to see if the rel has grown
		 * whilst we were vacuuming with non-exclusive lock.  If so, give up;
		 * the newly added pages presumably contain non-deletable tuples.
		 */
		new_rel_pages = RelationGetNumberOfBlocks(onerel);
		if (new_rel_pages != old_rel_pages)
		{
			/*
			 * Note: we intentionally don't update vacrelstats->rel_pages with
			 * the new rel size here.  If we did, it would amount to assuming
			 * that the new pages are empty, which is unlikely. Leaving the
			 * numbers alone amounts to assuming that the new pages have the
			 * same tuple density as existing ones, which is less unlikely.
			 */
			UnlockRelation(onerel, AccessExclusiveLock);
			return;
		}

		/*
		 * Scan backwards from the end to verify that the end pages actually
		 * contain no tuples.  This is *necessary*, not optional, because
		 * other backends could have added tuples to these pages whilst we
		 * were vacuuming.
		 */
		new_rel_pages = count_nondeletable_pages(onerel, vacrelstats);

		if (new_rel_pages >= old_rel_pages)
		{
			/* can't do anything after all */
			UnlockRelation(onerel, AccessExclusiveLock);
			return;
		}

		/*
		 * Okay to truncate.
		 */
		RelationTruncate(onerel, new_rel_pages);

		/*
		 * We can release the exclusive lock as soon as we have truncated.
		 * Other backends can't safely access the relation until they have
		 * processed the smgr invalidation that smgrtruncate sent out ... but
		 * that should happen as part of standard invalidation processing once
		 * they acquire lock on the relation.
		 */
		UnlockRelation(onerel, AccessExclusiveLock);

		/*
		 * Update statistics.  Here, it *is* correct to adjust rel_pages
		 * without also touching reltuples, since the tuple count wasn't
		 * changed by the truncation.
		 */
		vacrelstats->pages_removed += old_rel_pages - new_rel_pages;
		vacrelstats->rel_pages = new_rel_pages;

		ereport(elevel,
				(errmsg("\"%s\": truncated %u to %u pages",
						RelationGetRelationName(onerel),
						old_rel_pages, new_rel_pages),
				 errdetail("%s.",
						   pg_rusage_show(&ru0))));
		old_rel_pages = new_rel_pages;
	} while (new_rel_pages > vacrelstats->nonempty_pages &&
			 vacrelstats->lock_waiter_detected);
}

/*
 * Rescan end pages to verify that they are (still) empty of tuples.
 *
 * Returns number of nondeletable pages (last nonempty page + 1).
 */
static BlockNumber
count_nondeletable_pages(Relation onerel, LVRelStats *vacrelstats)
{
	BlockNumber blkno;
	instr_time	starttime;

	/* Initialize the starttime if we check for conflicting lock requests */
	INSTR_TIME_SET_CURRENT(starttime);

	/* Strange coding of loop control is needed because blkno is unsigned */
	blkno = vacrelstats->rel_pages;
	while (blkno > vacrelstats->nonempty_pages)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber offnum,
					maxoff;
		bool		hastup;

		/*
		 * Check if another process requests a lock on our relation. We are
		 * holding an AccessExclusiveLock here, so they will be waiting. We
		 * only do this once per VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL, and we
		 * only check if that interval has elapsed once every 32 blocks to
		 * keep the number of system calls and actual shared lock table
		 * lookups to a minimum.
		 */
		if ((blkno % 32) == 0)
		{
			instr_time	currenttime;
			instr_time	elapsed;

			INSTR_TIME_SET_CURRENT(currenttime);
			elapsed = currenttime;
			INSTR_TIME_SUBTRACT(elapsed, starttime);
			if ((INSTR_TIME_GET_MICROSEC(elapsed) / 1000)
				>= VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL)
			{
				if (LockHasWaitersRelation(onerel, AccessExclusiveLock))
				{
					ereport(elevel,
							(errmsg("\"%s\": suspending truncate due to conflicting lock request",
									RelationGetRelationName(onerel))));

					vacrelstats->lock_waiter_detected = true;
					return blkno;
				}
				starttime = currenttime;
			}
		}

		/*
		 * We don't insert a vacuum delay point here, because we have an
		 * exclusive lock on the table which we want to hold for as short a
		 * time as possible.  We still need to check for interrupts however.
		 */
		CHECK_FOR_INTERRUPTS();

		blkno--;

		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno,
								 RBM_NORMAL, vac_strategy);

		/* In this phase we only need shared access to the buffer */
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);

		if (PageIsNew(page) || PageIsEmpty(page))
		{
			/* PageIsNew probably shouldn't happen... */
			UnlockReleaseBuffer(buf);
			continue;
		}

		hastup = false;
		maxoff = PageGetMaxOffsetNumber(page);
		for (offnum = FirstOffsetNumber;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid;

			itemid = PageGetItemId(page, offnum);

			/*
			 * Note: any non-unused item should be taken as a reason to keep
			 * this page.  We formerly thought that DEAD tuples could be
			 * thrown away, but that's not so, because we'd not have cleaned
			 * out their index entries.
			 */
			if (ItemIdIsUsed(itemid))
			{
				hastup = true;
				break;			/* can stop scanning */
			}
		}						/* scan along page */

		UnlockReleaseBuffer(buf);

		/* Done scanning if we found a tuple here */
		if (hastup)
			return blkno + 1;
	}

	/*
	 * If we fall out of the loop, all the previously-thought-to-be-empty
	 * pages still are; we need not bother to look at the last known-nonempty
	 * page.
	 */
	return vacrelstats->nonempty_pages;
}

/*
 * lazy_space_alloc - space allocation decisions for lazy vacuum
 *
 * If we are in parallel lazy vacuum then the space for dead tuple
 * locatons are already allocated in DSM, so we allocate space for
 * dead tuple locations in local memory only when in not parallel
 * lazy vacuum. Also set MyDeadTuple.
 *
 * See the comments at the head of this file for rationale.
 */
static void
lazy_space_alloc(LVRelStats *vacrelstats, BlockNumber relblocks)
{
	/*
	 * If in not parallel lazy vacuum, we need to allocate dead
	 * tuple array in local memory.
	 */
	if (vacrelstats->pstate == NULL)
	{
		long maxtuples = lazy_get_max_dead_tuple(vacrelstats);
		fprintf(stderr, "--- maxtuples %ld ---\n", maxtuples);

		vacrelstats->dead_tuples = (LVDeadTuple *) palloc(sizeof(LVDeadTuple));
		MyDeadTuple = vacrelstats->dead_tuples;
		MyDeadTuple->dt_array = palloc0(sizeof(ItemPointerData) * (int)maxtuples);
		vacrelstats->max_dead_tuples = maxtuples;
	}
	else
	{
		/*
		 * Initialize the dead tuple array. LVDeadTuple array is structed at
		 * the beggning of dead_tuples variable, so remaining area is used for
		 * dead tuple array. The base variable points to the begnning of dead
		 * tuple array.
		 */

		char *dt_base = (char *)vacrelstats->dead_tuples;
		LVDeadTuple *dt = &(vacrelstats->dead_tuples[ParallelWorkerNumber]);

		/* Adjust dt_base to the end of LVDeadTuple array */
		dt_base += sizeof(LVDeadTuple) * vacrelstats->pstate->nworkers;
		dt->dt_array = (ItemPointer)
			(dt_base + sizeof(ItemPointerData) * vacrelstats->max_dead_tuples * ParallelWorkerNumber);

		MyDeadTuple = dt;

		/* @@@ for debugging */
		fprintf(stderr, "[%d] (%d) lvdt size  = %ld\n",
				MyProcPid,
				ParallelWorkerNumber,
				(char *) MyDeadTuple - (char *) vacrelstats->dead_tuples);
	}

	MyDeadTuple->n_dt = 0;
}

/*
 * lazy_record_dead_tuple - remember one deletable tuple
 */
static void
lazy_record_dead_tuple(LVRelStats *vacrelstats, ItemPointer itemptr)
{
	/*
	 * The array shouldn't overflow under normal behavior, but perhaps it
	 * could if we are given a really small maintenance_work_mem. In that
	 * case, just forget the last few tuples (we'll get 'em next time).
	 */
	if (MyDeadTuple->n_dt < vacrelstats->max_dead_tuples)
	{
		/*
		 * In parallel vacuum, since each paralell vacuum worker has own
		 * dead tuple array we don't need to do this exclusively even in
		 * parallel vacuum.
		 */
		MyDeadTuple->dt_array[MyDeadTuple->n_dt] = *itemptr;
		(MyDeadTuple->n_dt)++;

		/* XXX : Update progress information here */
	}
}

/*
 *  lazy_clear_dead_tuple() -- clear dead tuple list
 */
static void
lazy_clear_dead_tuple(LVRelStats *vacrelstats)
{
	/*
	 * In parallel vacuum, a parallel worker is responsible for
	 * clearing all dead tuples. Note that we're assumed that only
	 * one process touches the dead tuple array.
	 */
	if (vacrelstats->pstate != NULL && vacrelstats->nindexes != 0)
	{
		int i;
		for (i = 0; i < vacrelstats->pstate->nworkers; i++)
		{
			LVDeadTuple *dead_tuples = &(vacrelstats->dead_tuples[i]);
			dead_tuples->n_dt = 0;

			/* @@@ for debugging */
			fprintf(stderr, "[%d] (%d) clear tuple[%d] n_dt = %d\n",
					MyProcPid, ParallelWorkerNumber, i,
					dead_tuples->n_dt);
		}
	}
	else
	{
		/* @@@ for debugging */
		fprintf(stderr, "[%d] (%d) clear tuple n_dt = %d\n",
				MyProcPid, ParallelWorkerNumber,
				MyDeadTuple->n_dt);
		MyDeadTuple->n_dt = 0;
	}
}

/*
 *	lazy_tid_reaped() -- is a particular tid deletable?
 *
 *		This has the right signature to be an IndexBulkDeleteCallback.
 *
 *		Assumes dead_tuples array is in sorted order.
 */
static bool
lazy_tid_reaped(ItemPointer itemptr, void *state)
{
	LVRelStats *vacrelstats = (LVRelStats *) state;
	ItemPointer res;
	int i;
	int num = (vacrelstats->pstate == NULL) ? 1 :
		vacrelstats->pstate->nworkers;

	/*
	 * In parallel vacuum, all dead tuple TID address is stored into
	 * DSM together but entire that area is not order. However, since
	 * each dead tuple that are used by corresponding vacuum worker is
	 * ordered by TID address we can do bearch 'num' times.
	 */
	for (i = 0; i < num; i++)
	{
		ItemPointer dead_tuples = (vacrelstats->dead_tuples[i]).dt_array;
		int n_tuples = (vacrelstats->dead_tuples[i]).n_dt;

		res = (ItemPointer) bsearch((void *) itemptr,
									(void *) dead_tuples,
									n_tuples,
									sizeof(ItemPointerData),
									vac_cmp_itemptr);

		if (res != NULL)
			return true;
	}

	return false;
}

/*
 * Comparator routines for use with qsort() and bsearch().
 */
static int
vac_cmp_itemptr(const void *left, const void *right)
{
	BlockNumber lblk,
				rblk;
	OffsetNumber loff,
				roff;

	lblk = ItemPointerGetBlockNumber((ItemPointer) left);
	rblk = ItemPointerGetBlockNumber((ItemPointer) right);

	if (lblk < rblk)
		return -1;
	if (lblk > rblk)
		return 1;

	loff = ItemPointerGetOffsetNumber((ItemPointer) left);
	roff = ItemPointerGetOffsetNumber((ItemPointer) right);

	if (loff < roff)
		return -1;
	if (loff > roff)
		return 1;

	return 0;
}

/*
 * Check if every tuple in the given page is visible to all current and future
 * transactions. Also return the visibility_cutoff_xid which is the highest
 * xmin amongst the visible tuples.  Set *all_frozen to true if every tuple
 * on this page is frozen.
 */
static bool
heap_page_is_all_visible(Relation rel, Buffer buf,
						 TransactionId *visibility_cutoff_xid,
						 bool *all_frozen)
{
	Page		page = BufferGetPage(buf);
	BlockNumber blockno = BufferGetBlockNumber(buf);
	OffsetNumber offnum,
				maxoff;
	bool		all_visible = true;

	*visibility_cutoff_xid = InvalidTransactionId;
	*all_frozen = true;

	/*
	 * This is a stripped down version of the line pointer scan in
	 * lazy_scan_heap(). So if you change anything here, also check that code.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff && all_visible;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;
		HeapTupleData tuple;

		itemid = PageGetItemId(page, offnum);

		/* Unused or redirect line pointers are of no interest */
		if (!ItemIdIsUsed(itemid) || ItemIdIsRedirected(itemid))
			continue;

		ItemPointerSet(&(tuple.t_self), blockno, offnum);

		/*
		 * Dead line pointers can have index pointers pointing to them. So
		 * they can't be treated as visible
		 */
		if (ItemIdIsDead(itemid))
		{
			all_visible = false;
			*all_frozen = false;
			break;
		}

		Assert(ItemIdIsNormal(itemid));

		tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		tuple.t_len = ItemIdGetLength(itemid);
		tuple.t_tableOid = RelationGetRelid(rel);

		switch (HeapTupleSatisfiesVacuum(&tuple, OldestXmin, buf))
		{
			case HEAPTUPLE_LIVE:
				{
					TransactionId xmin;

					/* Check comments in lazy_scan_heap. */
					if (!HeapTupleHeaderXminCommitted(tuple.t_data))
					{
						all_visible = false;
						*all_frozen = false;
						break;
					}

					/*
					 * The inserter definitely committed. But is it old enough
					 * that everyone sees it as committed?
					 */
					xmin = HeapTupleHeaderGetXmin(tuple.t_data);
					if (!TransactionIdPrecedes(xmin, OldestXmin))
					{
						all_visible = false;
						*all_frozen = false;
						break;
					}

					/* Track newest xmin on page. */
					if (TransactionIdFollows(xmin, *visibility_cutoff_xid))
						*visibility_cutoff_xid = xmin;

					/* Check whether this tuple is already frozen or not */
					if (all_visible && *all_frozen &&
						heap_tuple_needs_eventual_freeze(tuple.t_data))
						*all_frozen = false;
				}
				break;

			case HEAPTUPLE_DEAD:
			case HEAPTUPLE_RECENTLY_DEAD:
			case HEAPTUPLE_INSERT_IN_PROGRESS:
			case HEAPTUPLE_DELETE_IN_PROGRESS:
				{
					all_visible = false;
					*all_frozen = false;
					break;
				}
			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				break;
		}
	}							/* scan along page */

	return all_visible;
}

/*
 * Return the block number we need to scan, or InvalidBlockNumber if scan
 * is done.
 *
 * Except when aggressive is set, we want to skip pages that are
 * all-visible according to the visibility map, but only when we can skip
 * at least SKIP_PAGES_THRESHOLD consecutive pages If we're not in parallel
 * mode.  Since we're reading sequentially, the OS should be doing readahead
 * for us, so there's no gain in skipping a page now and then; that's likely
 * to disable readahead and so be counterproductive. Also, skipping even a
 * single page means that we can't update relfrozenxid, so we only want to do it
 * if we can skip a goodly number of pages.
 *
 * When aggressive is set, we can't skip pages just because they are
 * all-visible, but we can still skip pages that are all-frozen, since
 * such pages do not need freezing and do not affect the value that we can
 * safely set for relfrozenxid or relminmxid.
 *
 * In not parallel mode, before entering the main loop, establish the
 * invariant that next_unskippable_block is the next block number >= blkno
 * that's not we can't skip based on the visibility map, either all-visible
 * for a regular scan or all-frozen for an aggressive scan.  We set it to
 * nblocks if there's no such block.  We also set up the skipping_blocks
 * flag correctly at this stage.
 *
 * In parallel mode, we scan heap pages using parallel heap scan infrastructure.
 * Each worker calls heap_parallelscan_nextpage() in order to get exclusively
 * block number we need to scan at next. If given block is all-visible
 * according to visibility map, we skip to scan this block immediately unlike
 * not parallelly lazy scan.
 *
 * Note: The value returned by visibilitymap_get_status could be slightly
 * out-of-date, since we make this test before reading the corresponding
 * heap page or locking the buffer.  This is OK.  If we mistakenly think
 * that the page is all-visible or all-frozen when in fact the flag's just
 * been cleared, we might fail to vacuum the page.  It's easy to see that
 * skipping a page when aggressive is not set is not a very big deal; we
 * might leave some dead tuples lying around, but the next vacuum will
 * find them.  But even when aggressive *is* set, it's still OK if we miss
 * a page whose all-frozen marking has just been cleared.  Any new XIDs
 * just added to that page are necessarily newer than the GlobalXmin we
 * computed, so they'll have no effect on the value to which we can safely
 * set relfrozenxid.  A similar argument applies for MXIDs and relminmxid.
 *
 * We will scan the table's last page, at least to the extent of
 * determining whether it has tuples or not, even if it should be skipped
 * according to the above rules; except when we've already determined that
 * it's not worth trying to truncate the table.  This avoids having
 * lazy_truncate_heap() take access-exclusive lock on the table to attempt
 * a truncation that just fails immediately because there are tuples in
 * the last page.  This is worth avoiding mainly because such a lock must
 * be replayed on any hot standby, where it can be disruptive.
 */
static BlockNumber
lazy_scan_heap_get_nextpage(Relation onerel, LVRelStats *vacrelstats,
							LVScanDesc lvscan, bool *all_visible_according_to_vm,
							Buffer *vmbuffer, int options, bool aggressive)
{
	BlockNumber blkno;

	if (vacrelstats->pstate != NULL)
	{
		/*
		 * In parallel vacuum, since it's hard to know how many consecutive all-visible
		 * pages exits on this relation, we skip to scan the heap page immidiately.
		 */
		while ((blkno = heap_parallelscan_nextpage(lvscan->heapscan)) != InvalidBlockNumber)
		{
			*all_visible_according_to_vm = false;

			/* Consider to skip scan page according visibility map */
			if ((options & VACOPT_DISABLE_PAGE_SKIPPING) == 0 &&
				!FORCE_CHECK_PAGE(blkno))
			{
				uint8		vmstatus;

				vmstatus = visibilitymap_get_status(onerel, blkno, vmbuffer);

				if (aggressive)
				{
					if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) != 0)
					{
						vacrelstats->frozenskipped_pages++;
						continue;
					}
					else if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) != 0)
						*all_visible_according_to_vm = true;
				}
				else
				{
					if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) != 0)
					{
						if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) == 0)
							vacrelstats->frozenskipped_pages++;
						continue;
					}
				}
			}

			/* We need to scan current blkno, break */
			break;
		}
	}
	else
	{
		bool skipping_blocks = false;

		/* Initialize lv_nextunskippable_page if needed */
		if (lvscan->lv_cblock == 0 && (options & VACOPT_DISABLE_PAGE_SKIPPING) == 0)
		{
			while (lvscan->lv_next_unskippable_block < lvscan->lv_nblocks)
			{
				uint8		vmstatus;

				vmstatus = visibilitymap_get_status(onerel, lvscan->lv_next_unskippable_block,
													vmbuffer);
				if (aggressive)
				{
					if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) == 0)
						break;
				}
				else
				{
					if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) == 0)
						break;
				}
				vacuum_delay_point();
				lvscan->lv_next_unskippable_block++;
			}

			if (lvscan->lv_next_unskippable_block >= SKIP_PAGES_THRESHOLD)
				skipping_blocks = true;
			else
				skipping_blocks = false;
		}

		/* Decide the block number we need to scan */
		for (blkno = lvscan->lv_cblock; blkno < lvscan->lv_nblocks; blkno++)
		{
			if (blkno == lvscan->lv_next_unskippable_block)
			{
				/* Time to advance next_unskippable_block */
				lvscan->lv_next_unskippable_block++;
				if ((options & VACOPT_DISABLE_PAGE_SKIPPING) == 0)
				{
					while (lvscan->lv_next_unskippable_block < lvscan->lv_nblocks)
					{
						uint8		vmstatus;

						vmstatus = visibilitymap_get_status(onerel,
															lvscan->lv_next_unskippable_block,
															vmbuffer);
						if (aggressive)
						{
							if ((vmstatus & VISIBILITYMAP_ALL_FROZEN) == 0)
								break;
						}
						else
						{
							if ((vmstatus & VISIBILITYMAP_ALL_VISIBLE) == 0)
								break;
						}
						vacuum_delay_point();
						lvscan->lv_next_unskippable_block++;
					}
				}

				/*
				 * We know we can't skip the current block.  But set up
				 * skipping_all_visible_blocks to do the right thing at the
				 * following blocks.
				 */
				if (lvscan->lv_next_unskippable_block - blkno > SKIP_PAGES_THRESHOLD)
					skipping_blocks = true;
				else
					skipping_blocks = false;

				/*
				 * Normally, the fact that we can't skip this block must mean that
				 * it's not all-visible.  But in an aggressive vacuum we know only
				 * that it's not all-frozen, so it might still be all-visible.
				 */
				if (aggressive && VM_ALL_VISIBLE(onerel, blkno, vmbuffer))
					*all_visible_according_to_vm = true;

				/* Found out that next unskippable block number */
				break;
			}
			else
			{
				/*
				 * The current block is potentially skippable; if we've seen a
				 * long enough run of skippable blocks to justify skipping it, and
				 * we're not forced to check it, then go ahead and skip.
				 * Otherwise, the page must be at least all-visible if not
				 * all-frozen, so we can set all_visible_according_to_vm = true.
				 */
				if (skipping_blocks && !FORCE_CHECK_PAGE(blkno))
				{
					/*
					 * Tricky, tricky.  If this is in aggressive vacuum, the page
					 * must have been all-frozen at the time we checked whether it
					 * was skippable, but it might not be any more.  We must be
					 * careful to count it as a skipped all-frozen page in that
					 * case, or else we'll think we can't update relfrozenxid and
					 * relminmxid.  If it's not an aggressive vacuum, we don't
					 * know whether it was all-frozen, so we have to recheck; but
					 * in this case an approximate answer is OK.
					 */
					if (aggressive || VM_ALL_FROZEN(onerel, blkno, vmbuffer))
						vacrelstats->frozenskipped_pages++;
					continue;
				}

				*all_visible_according_to_vm = true;

				/* We need to scan current blkno, break */
				break;
			}
		} /* for */

		/* Advance the current block number for the next scan */
		lvscan->lv_cblock = blkno + 1;
	}

	return (blkno == lvscan->lv_nblocks) ? InvalidBlockNumber : blkno;
}

/*
 * Begin lazy vacuum scan. lvscan->heapscan is NULL if
 * we're not in parallel lazy vacuum.
 */
static LVScanDesc
lv_beginscan(LVRelStats *vacrelstats, ParallelHeapScanDesc pscan,
			 Relation onerel)
{
	LVScanDesc lvscan;

	lvscan = (LVScanDesc) palloc(sizeof(LVScanDescData));

	lvscan->lv_cblock = 0;
	lvscan->lv_next_unskippable_block = 0;
	lvscan->lv_nblocks = vacrelstats->rel_pages;

	if (pscan != NULL)
		lvscan->heapscan = heap_beginscan_parallel(onerel, pscan);
	else
		lvscan->heapscan = NULL;

	return lvscan;
}

/*
 * End lazy vacuum scan.
 */
static void
lv_endscan(LVScanDesc lvscan)
{
	if (lvscan->heapscan != NULL)
		heap_endscan(lvscan->heapscan);
	pfree(lvscan);
}

/* ----------------------------------------------------------------
 *						Parallel Lazy Vacuum Support
 * ----------------------------------------------------------------
 */

/*
 * Estimate storage for parallel lazy vacuum.
 */
static void
lazy_estimate_dsm(ParallelContext *pcxt, long maxtuples, int nindexes)
{
	int size = 0;
	int keys = 0;

	/* Estimate size for parallel heap scan */
	size += heap_parallelscan_estimate(SnapshotAny);
	keys++;

	/* Estimate size for vacuum statistics */
	size += BUFFERALIGN(sizeof(LVRelStats) * pcxt->nworkers);
	keys++;

	/* Estimate size fo index vacuum statistics */
	size += BUFFERALIGN(sizeof(LVIndStats) * nindexes);
	keys++;

	/* Estimate size for dead tuple arrays */
	size += BUFFERALIGN((sizeof(LVDeadTuple) + sizeof(ItemPointerData) * maxtuples) * pcxt->nworkers);
	keys++;

	/* Estimate size for parallel lazy vacuum state */
	size += BUFFERALIGN(sizeof(LVParallelState) + sizeof(VacWorker) * pcxt->nworkers);
	keys++;

	/* Estimate size for vacuum task */
	size += BUFFERALIGN(sizeof(VacuumTask));
	keys++;

	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, keys);
}

/*
 * Initialize dynamic shared memory for parallel lazy vacuum. We store
 * relevant informations of parallel heap scanning, dead tuple array
 * and vacuum statistics for each worker and some parameters for
 * lazy vacuum.
 */
static void
lazy_initialize_dsm(ParallelContext *pcxt, Relation onerel,
					LVRelStats *vacrelstats, int options,
					bool aggressive)
{
	ParallelHeapScanDesc pscan;
	LVRelStats *lvrelstats;
	LVIndStats *lvindstats;
	LVDeadTuple *dead_tuples;
	LVParallelState *pstate;
	VacuumTask	*vacuum_task;
	int i;
	int dead_tuples_size;
	int pstate_size;

	/* Allocate and intialize DSM for parallel scan description */
	pscan = (ParallelHeapScanDesc) shm_toc_allocate(pcxt->toc,
													heap_parallelscan_estimate(SnapshotAny));

	shm_toc_insert(pcxt->toc, VACUUM_KEY_PARALLEL_SCAN, pscan);
	heap_parallelscan_initialize(pscan, onerel, SnapshotAny);

	/* Allocate and initialize DSM for vacuum stats for each worker */
	lvrelstats = (LVRelStats *)shm_toc_allocate(pcxt->toc,
											 sizeof(LVRelStats) * pcxt->nworkers);
	shm_toc_insert(pcxt->toc, VACUUM_KEY_VACUUM_STATS, lvrelstats);
	for (i = 0; i < pcxt->nworkers; i++)
	{
		LVRelStats *stats = lvrelstats + sizeof(LVRelStats) * i;

		memcpy(stats, vacrelstats, sizeof(LVRelStats));
	}

	/* Allocate and initialize DSM for dead tuple array */
	dead_tuples_size = sizeof(LVDeadTuple) * pcxt->nworkers;
	dead_tuples_size += sizeof(ItemPointerData) * vacrelstats->max_dead_tuples * pcxt->nworkers;
	dead_tuples = (LVDeadTuple *) shm_toc_allocate(pcxt->toc, dead_tuples_size);
	vacrelstats->dead_tuples = dead_tuples;
	shm_toc_insert(pcxt->toc, VACUUM_KEY_DEAD_TUPLES, dead_tuples);

	/* Allocate DSM for index vacuum statistics */
	lvindstats = (LVIndStats *) shm_toc_allocate(pcxt->toc,
												 sizeof(LVIndStats) * vacrelstats->nindexes);
	shm_toc_insert(pcxt->toc, VACUUM_KEY_INDEX_STATS, lvindstats);


	/* Allocate and initialize DSM for paralle state */
	pstate_size = sizeof(LVParallelState) + sizeof(VacWorker) * pcxt->nworkers;
	pstate = (LVParallelState *) shm_toc_allocate(pcxt->toc, pstate_size);
	shm_toc_insert(pcxt->toc, VACUUM_KEY_PARALLEL_STATE, pstate);
	pstate->nworkers = pcxt->nworkers;
	ConditionVariableInit(&(pstate->cv));
	SpinLockInit(&(pstate->mutex));

	/* Allocate and initialize DSM for vacuum task */
	vacuum_task = (VacuumTask *) shm_toc_allocate(pcxt->toc, sizeof(VacuumTask));
	shm_toc_insert(pcxt->toc, VACUUM_KEY_VACUUM_TASK, vacuum_task);
	vacuum_task->aggressive = aggressive;
	vacuum_task->options = options;
	vacuum_task->oldestxmin = OldestXmin;
	vacuum_task->freezelimit = FreezeLimit;
	vacuum_task->multixactcutoff = MultiXactCutoff;
	vacuum_task->elevel = elevel;
}

/*
 * Initialize parallel lazy vacuum for worker.
 */
static void
lazy_initialize_worker(shm_toc *toc, ParallelHeapScanDesc *pscan,
						   LVRelStats **vacrelstats, int *options,
						   bool *aggressive)
{
	LVRelStats *lvstats;
	LVIndStats *vacindstats;
	VacuumTask	*vacuum_task;
	LVDeadTuple *dead_tuples;
	LVParallelState *pstate;

	/* Set up parallel heap scan description */
	*pscan = (ParallelHeapScanDesc) shm_toc_lookup(toc, VACUUM_KEY_PARALLEL_SCAN);

	/* Set up vacuum stats */
	lvstats = (LVRelStats *) shm_toc_lookup(toc, VACUUM_KEY_VACUUM_STATS);
	*vacrelstats = lvstats + sizeof(LVRelStats) * ParallelWorkerNumber;

	/* Set up vacuum index statistics */
	vacindstats = (LVIndStats *) shm_toc_lookup(toc, VACUUM_KEY_INDEX_STATS);
	(*vacrelstats)->vacindstats = (LVIndStats *)vacindstats;

	/* Set up dead tuple list */
	dead_tuples = (LVDeadTuple *) shm_toc_lookup(toc, VACUUM_KEY_DEAD_TUPLES);
	(*vacrelstats)->dead_tuples = dead_tuples;

	/* Set up vacuum task */
	vacuum_task = (VacuumTask *) shm_toc_lookup(toc, VACUUM_KEY_VACUUM_TASK);

	/* Set up parallel vacuum state */
	pstate = (LVParallelState *) shm_toc_lookup(toc, VACUUM_KEY_PARALLEL_STATE);
	(*vacrelstats)->pstate = pstate;
	MyVacWorker = &(pstate->vacworkers[ParallelWorkerNumber]);
	MyVacWorker->state = VACSTATE_STARTUP;

	/* Set up parameters for lazy vacuum */
	OldestXmin = vacuum_task->oldestxmin;
	FreezeLimit = vacuum_task->freezelimit;
	MultiXactCutoff = vacuum_task->multixactcutoff;
	elevel = vacuum_task->elevel;
	*options = vacuum_task->options;
	*aggressive = vacuum_task->aggressive;
}

/*
 * Set my vacuum state exclusively and wait until its state is changed
 * by arbiter process.
 */
static void
lazy_set_vacstate_and_wait_finished(LVRelStats *vacrelstats)
{
	LVParallelState *pstate = vacrelstats->pstate;
	uint32 round;
	int n_count, n_comp;

	/* Exit if in not parallel vacuum */
	if (pstate == NULL)
		return;

	SpinLockAcquire(&(pstate->mutex));

	/* Change my vacstate */
	round = MyVacWorker->round;
	MyVacWorker->state = VACSTATE_VACUUM_FINISHED;
	//round = lazy_set_my_vacstate(pstate, VACSTATE_VACUUM_FINISHED, false, false);

	/* If I'm a last worker to reached here */
	n_count = lazy_count_vacstate_finished(pstate, round, &n_comp);

	if ((n_count + n_comp) == pstate->nworkers)
		lazy_clear_dead_tuple(vacrelstats);

	SpinLockRelease(&(pstate->mutex));

	/* Sleep until my vacstate is changed to next state by arbiter process */
	ConditionVariablePrepareToSleep(&(pstate->cv));
	while (!lazy_check_vacstate_finished(pstate, round))
	{
		fprintf(stderr, "[%d] (%d) finished - Sleep... bye\n",
				MyProcPid, ParallelWorkerNumber);
		ConditionVariableSleep(&(pstate->cv), WAIT_EVENT_PARALLEL_FINISH);
		fprintf(stderr, "[%d] (%d) finished - wake up try again\n",
				MyProcPid, ParallelWorkerNumber);
	}
	ConditionVariableCancelSleep();

	lazy_set_my_vacstate(pstate, VACSTATE_SCANNING, true, false);

	/* @@@ for debugging */
	fprintf(stderr, "[%d] worker finished - %d resume next round\n",
			MyProcPid, ParallelWorkerNumber);
}

static void
lazy_set_vacstate_and_wait_prepared(LVParallelState *pstate)
{
	uint32 round;

	/* Exit if in not parallel vacuum */
	if (pstate == NULL)
		return;

	/* update my vacstate */
	round = lazy_set_my_vacstate(pstate, VACSTATE_VACUUM_PREPARED, false, true);

	/* Sleep until my vacstate is changed to next state by arbiter process */
	ConditionVariablePrepareToSleep(&(pstate->cv));

	while (!lazy_check_vacstate_prepared(pstate, round))
	{
		fprintf(stderr, "[%d] (%d) prepare - Sleep... bye\n",
				MyProcPid, ParallelWorkerNumber);
		ConditionVariableSleep(&(pstate->cv), WAIT_EVENT_PARALLEL_FINISH);
		fprintf(stderr, "[%d] (%d) prepare - wake up try again\n",
				MyProcPid, ParallelWorkerNumber);
	}
	ConditionVariableCancelSleep();

	/* @@@ for debugging */
	fprintf(stderr, "[%d] worker prepared - %d resume\n",
			MyProcPid, ParallelWorkerNumber);

	lazy_set_my_vacstate(pstate, VACSTATE_VACUUMING, false, false);
}

/*
 * Set my vacstate. Since the arbiter process could touch all of vacstate
 * we need to get my vacstate exclusively. After set my state, wake other
 * waiting process if required. This process must be called by vacuum worker
 * process.
 */
static uint32
lazy_set_my_vacstate(LVParallelState *pstate, uint8 state, bool nextloop,
					 bool broadcast)
{
	uint32 round;

	/* Quick exit if in not parallle vacuum */
	if (pstate == NULL)
		return 0;

	Assert(IsParallelWorker());

	SpinLockAcquire(&(pstate->mutex));

	MyVacWorker->state = state;
	round = MyVacWorker->round;
	
	SpinLockRelease(&(pstate->mutex));

	if (nextloop)
		(MyVacWorker->round)++;

	if (broadcast)
		ConditionVariableBroadcast(&(pstate->cv));

	fprintf(stderr, "[%d] (%d) state changed to %d loops = %u\n",
			MyProcPid, ParallelWorkerNumber, state,
			MyVacWorker->round);
	return round;
}

static bool
lazy_check_vacstate_prepared(LVParallelState *pstate, uint32 round)
{
	int n_count = 0;
	int n_comp = 0;
	int i;
	uint8 countable_state = VACSTATE_VACUUM_PREPARED | VACSTATE_VACUUMING |
		VACSTATE_VACUUM_FINISHED;

	SpinLockAcquire(&(pstate->mutex));

	for (i = 0; i < pstate->nworkers; i++)
	{
		VacWorker *vacworker = &(pstate->vacworkers[i]);
		uint32 w_round = vacworker->round;

		if ((vacworker->state & countable_state) != 0 && w_round ==  round)
			n_count++;
		else if (vacworker->state == VACSTATE_COMPLETE)
			n_comp++;
	}

	SpinLockRelease(&(pstate->mutex));

	fprintf(stderr, "[%d] (%d) prepared count n = %d, comp = %d\n",
			MyProcPid, ParallelWorkerNumber, n_count, n_comp);
	
	return (n_count + n_comp) == pstate->nworkers;
}

static bool
lazy_check_vacstate_finished(LVParallelState *pstate, uint32 round)
{
	int n_count, n_comp;

	SpinLockAcquire(&(pstate->mutex));
	n_count = lazy_count_vacstate_finished(pstate, round, &n_comp);
	SpinLockRelease(&(pstate->mutex));

	fprintf(stderr, "[%d] (%d) finished count n = %d, comp = %d\n",
			MyProcPid, ParallelWorkerNumber, n_count, n_comp);
	return (n_count + n_comp) == pstate->nworkers;
}

/*
 * Count the number of vacuum worker that is in the same state or is in
 * ahread state on next round.
 * Caller must hold mutex lock.
 */
static int
lazy_count_vacstate_finished(LVParallelState *pstate, uint32 round, int *n_complete)
{
	int n_count = 0;
	int n_comp = 0;
	int i;
	uint8 countable_cur_state = VACSTATE_VACUUM_FINISHED;
	uint8 countable_next_state = VACSTATE_SCANNING | VACSTATE_VACUUM_PREPARED;

	for (i = 0; i < pstate->nworkers; i++)
	{
		VacWorker *vacworker = &(pstate->vacworkers[i]);
		uint32 w_round = vacworker->round;

		if (((vacworker->state & countable_cur_state) != 0 && w_round == round) ||
			((vacworker->state & countable_next_state) != 0 && w_round == (round + 1)))
			n_count++;
		else if (vacworker->state == VACSTATE_COMPLETE)
			n_comp++;
	}

	*n_complete = n_comp;

	return n_count;
}

/*
 * Return the number of maximun dead tuples can be stored accroding
 * to vac_work_mem.
 */
static long
lazy_get_max_dead_tuple(LVRelStats *vacrelstats)
{
	long maxtuples;
	int	vac_work_mem = IsAutoVacuumWorkerProcess() &&
		autovacuum_work_mem != -1 ?
		autovacuum_work_mem : maintenance_work_mem;

	if (vacrelstats->nindexes != 0)
	{
		maxtuples = (vac_work_mem * 1024L) / sizeof(ItemPointerData);
		maxtuples = Min(maxtuples, INT_MAX);
		maxtuples = Min(maxtuples, MaxAllocSize / sizeof(ItemPointerData));

		/* curious coding here to ensure the multiplication can't overflow */
		if ((BlockNumber) (maxtuples / LAZY_ALLOC_TUPLES) > vacrelstats->old_rel_pages)
			maxtuples = vacrelstats->old_rel_pages * LAZY_ALLOC_TUPLES;

		/* stay sane if small maintenance_work_mem */
		maxtuples = Max(maxtuples, MaxHeapTuplesPerPage);
	}
	else
	{
		maxtuples = MaxHeapTuplesPerPage;
	}

	return maxtuples;
}
