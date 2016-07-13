/* -------------------------------------------------------------------------
 *
 * pg_subscription.h
 *		Definition of the subscription catalog (pg_subscription).
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_H
#define PG_SUBSCRIPTION_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_subscription definition. cpp turns this into
 *		typedef struct FormData_pg_subscription
 * ----------------
 */
#define SubscriptionRelationId			6100
#define SubscriptionRelation_Rowtype_Id	6101

CATALOG(pg_subscription,6100) BKI_SHARED_RELATION BKI_ROWTYPE_OID(6101) BKI_SCHEMA_MACRO
{
	Oid			subdbid;			/* Database the subscription is in. */
	NameData	subname;		/* Name of the subscription */
	bool		subenabled;		/* True if the subsription is enabled (running) */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		subconninfo;	/* Connection string to the provider */
	NameData	subslotname;	/* Slot name on provider */

	name		subpublications[1];	/* List of publications subscribed to */
#endif
} FormData_pg_subscription;

typedef FormData_pg_subscription *Form_pg_subscription;

/* ----------------
 *		compiler constants for pg_subscription
 * ----------------
 */
#define Natts_pg_subscription				6
#define Anum_pg_subscription_subdbid		1
#define Anum_pg_subscription_subname		2
#define Anum_pg_subscription_subenabled		3
#define Anum_pg_subscription_subconninfo	4
#define Anum_pg_subscription_subslotname	5
#define Anum_pg_subscription_subpublications	6


typedef struct Subscription
{
	Oid		oid;			/* Oid of the subscription */
	Oid		dbid;			/* Oid of the database which dubscription is in */
	char   *name;			/* Name of the subscription */
	bool	enabled;		/* Indicates if the subscription is enabled */
	char   *conninfo;		/* Connection string to the provider */
	char   *slotname;		/* Name of the replication slot */
	List   *publications;	/* List of publication names to subscribe to */
} Subscription;

extern Subscription *GetSubscription(Oid subid, bool missing_ok);
extern void FreeSubscription(Subscription *sub);
extern Oid get_subscription_oid(const char *subname, bool missing_ok);

#endif   /* PG_SUBSCRIPTION_H */
