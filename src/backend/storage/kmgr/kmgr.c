/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 This module manages the master encryption key.
 *
 * In transparent data encryption we have one master key for the whole
 * database cluster. It is used to encrypt and decrypt each tablespace keys.
 *
 * When postmaster startups, it load the kmgr plugin specified by
 * kmgr_plugin_library, and then takes the master key via the getkey callback
 * with the master key id generated by the system identifier and sequence
 * number starting from 0. If the plugin could not find the master key we request
 * to generate the new master key with the key identifier. The fetched master
 * key is stored in the shared memory space and shared among all postgres
 * processes.
 *
 * When key rotation, we request to the plugin to generate new master key with
 * the key identifier whose sequent number is incremented.
 *
 * XXX : when remove, error handling, locking for key rotation
 *
 *  Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/keyring/master_key.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "fmgr.h"

#include "access/xlog.h"
#include "storage/buf_internals.h"
#include "storage/kmgr.h"
#include "storage/kmgr_api.h"
#include "storage/kmgr_plugin.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/*
 * The master key format is "pg_master_key-<database systemid>-<seqno>".
 * The maximum length of database system identifer is
 * 20 (=18446744073709551615) as it is an uint64 value and the maximum
 * string length of seqno is 10 (=4294967295).
 */
#define MASTERKEY_ID_FORMAT "pg_master_key-%7lu-%04u"
#define MASTERKEY_ID_FORMAT_SCAN "pg_master_key-%lu-%s"

#define FIRST_MASTERKEY_SEQNO	0

/*
 * Shared memory structer for master key.
 */
typedef struct KmgrCtlData
{
	MasterKeySeqNo	seqno;
	slock_t			mutex;	/* protect above fields */
} KmgrCtlData;
static KmgrCtlData *kmgrCtl = NULL;

/* GUC variable */
char *kmgr_plugin_library = NULL;

PG_FUNCTION_INFO_V1(pg_rotate_encryption_key);

/* process and load kmgr_plugin_library plugin */
void
processKmgrPlugin(void)
{
	process_shared_preload_libraries_in_progress = true;
	startupKmgrPlugin(kmgr_plugin_library);
	process_shared_preload_libraries_in_progress = false;
}

/*
 * Get the master key via kmgr plugin, and store both key and id to the
 * shared memory. This function must be used at postmaster startup time
 * but after created shared memory.
 */
void
InitializeKmgr(void)
{
	char id[MASTER_KEY_ID_LEN] = {0};
	MasterKeySeqNo seqno;
	char *key = NULL;

	if (!TransparentEncryptionEnabled())
		return;

	/* Invoke startup callback */
	KmgrPluginStartup();

	/* Read keyring file and get the master key id */
	if (!getMasterKeyIdFromFile(id))
	{
		/* First time, create initial identifier */
		seqno = FIRST_MASTERKEY_SEQNO;
		snprintf(id, MASTER_KEY_ID_LEN, MASTERKEY_ID_FORMAT,
				 GetSystemIdentifier(), seqno);
	}
	else
	{
		uint64	dummy;
		char	seqno_str[5];

		/* Got the maste key id, got sequence number */
		sscanf(id, MASTERKEY_ID_FORMAT_SCAN,  &dummy, seqno_str);
		seqno = atoi(seqno_str);

#ifdef DEBUG_TDE
		fprintf(stderr, "kmgr::initialize found keyring file, id %s seqno_str %s seqno %u\n",
			id, seqno_str, seqno);
#endif
	}

	Assert(seqno >= 0);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmtr::initialize startup mkid %s, seqno %u\n",
			id, seqno);
#endif

	if (!KmgrPluginIsExist(id))
		KmgrPluginGenerateKey(id);

	/* Get the master key from plugin */
	KmgrPluginGetKey(id, &key);

	if (key == NULL)
		ereport(FATAL,
				(errmsg("could not get the encryption master key via kmgr plugin")));

	/* Save current master key seqno */
	kmgrCtl->seqno = seqno;

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::initialize set id %s, key %s, seq %u\n",
			id, dk(key), kmgrCtl->seqno);
#endif
}

Size
KmgrCtlShmemSize(void)
{
	return sizeof(KmgrCtlData);
}

void
KmgrCtlShmemInit(void)
{
	bool		found;

	/* Create shared memory struct for master keyring */
	kmgrCtl = (KmgrCtlData *) ShmemInitStruct("Encryption key management",
											  KmgrCtlShmemSize(),
											  &found);

	if (!found)
	{
		/* Initialize */
		MemSet(kmgrCtl, 0, KmgrCtlShmemSize());
		SpinLockInit(&kmgrCtl->mutex);
	}
}

MasterKeySeqNo
GetMasterKeySeqNo(void)
{
	MasterKeySeqNo seqno;

	Assert(kmgrCtl);

	SpinLockAcquire(&kmgrCtl->mutex);
	seqno = kmgrCtl->seqno;
	SpinLockRelease(&kmgrCtl->mutex);

	return seqno;
}

char *
GetMasterKey(const char *id)
{
	char *key = NULL;

	KmgrPluginGetKey(id, &key);

	return key;
}

void
GetCurrentMasterKeyId(char *keyid)
{
	sprintf(keyid, MASTERKEY_ID_FORMAT,
			GetSystemIdentifier(), GetMasterKeySeqNo());
}

/*
 * Rotate the master key and reencrypt all tablespace keys with new one.
 */
Datum
pg_rotate_encryption_key(PG_FUNCTION_ARGS)
{
	char newid[MASTER_KEY_ID_LEN + 1] = {0};
	char *newkey = NULL;
	MasterKeySeqNo seqno;

	/* prevent concurrent process trying key rotation */
	LWLockAcquire(MasterKeyRotationLock, LW_EXCLUSIVE);

	/* Craft the new master key id */
	seqno = GetMasterKeySeqNo();
	sprintf(newid, MASTERKEY_ID_FORMAT, GetSystemIdentifier(), seqno + 1);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::rotate new id id %s, oldseq %u\n",
			newid, seqno);
#endif

	/* Get new master key */
	KmgrPluginGenerateKey(newid);
	KmgrPluginGetKey(newid, &newkey);

#ifdef DEBUG_TDE
	fprintf(stderr, "kmgr::rotate generated new id id %s, key %s\n",
			newid, dk(newkey));
#endif

	/* Block concurrent processes are about to read the keyring file */
	LWLockAcquire(KeyringControlLock, LW_EXCLUSIVE);

	/*
	 * Reencrypt all tablespace keys with the new master key, and update
	 * the keyring file.
	 */
	reencryptKeyring(newid, newkey);

	/* Update master key information */
	SpinLockAcquire(&kmgrCtl->mutex);
	kmgrCtl->seqno = seqno + 1;
	SpinLockRelease(&kmgrCtl->mutex);

	/* Ok allows processes to read the keyring file */
	LWLockRelease(KeyringControlLock);

	/* Invalidate keyring caches before releasing the lock */
	SysCacheInvalidate(TABLESPACEOID, (Datum) 0);

	LWLockRelease(MasterKeyRotationLock);

	PG_RETURN_TEXT_P(cstring_to_text(newid));
}
