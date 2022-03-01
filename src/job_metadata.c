/*-------------------------------------------------------------------------
 *
 * src/job_metadata.c
 *
 * Functions for reading and manipulating pg_cron metadata.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "cron.h"
#include "pg_cron.h"
#include "job_metadata.h"
#include "cron_job.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_extension.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "postmaster/postmaster.h"
#include "pgstat.h"
#include "storage/lock.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#if (PG_VERSION_NUM >= 100000)
#include "utils/varlena.h"
#endif

#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "catalog/pg_authid.h"

#if (PG_VERSION_NUM < 120000)
#define table_open(r, l) heap_open(r, l)
#define table_close(r, l) heap_close(r, l)
#endif

#define EXTENSION_NAME "pg_cron"
#define CRON_SCHEMA_NAME "cron"
#define JOBS_TABLE_NAME "job"
#define JOB_ID_INDEX_NAME "job_pkey"
#define JOB_ID_SEQUENCE_NAME "cron.jobid_seq"
#define JOB_RUN_DETAILS_TABLE_NAME "job_run_details"
#define RUN_ID_SEQUENCE_NAME "cron.runid_seq"
#define LT_JOB_EXT "lt_job_ext"

#define DEFAULT_TIME_ZONE	"8"

/* forward declarations */
static HTAB * CreateCronJobHash(void);


static int64 ScheduleCronJob(text *scheduleText, text *commandText,
								text *databaseText, text *usernameText,
								bool active, text *jobnameText, char *mode, char *tmzone);
static Oid CronExtensionOwner(void);
static void EnsureDeletePermission(Relation cronJobsTable, HeapTuple heapTuple);
static void InvalidateJobCacheCallback(Datum argument, Oid relationId);
static void InvalidateJobCache(void);
static Oid CronJobRelationId(void);

static CronJob * TupleToCronJob(TupleDesc tupleDescriptor, HeapTuple heapTuple);
static bool PgCronHasBeenLoaded(void);
static bool JobRunDetailsTableExists(void);
static bool JobTableExists(void);

static void AlterJob(int64 jobId, text *scheduleText, text *commandText,
						text *databaseText, text *usernameText, bool *active);

static Oid GetRoleOidIfCanLogin(char *username);

static bool JobLtExtTableExists(void);
static void insertCronExt(int64 jobid, char *jobname, char *mode, char *tmzone);
static void deleteCronExt(int64 jobid, char *jobname);

/* SQL-callable functions */
PG_FUNCTION_INFO_V1(cron_schedule);
PG_FUNCTION_INFO_V1(cron_schedule_named);
PG_FUNCTION_INFO_V1(cron_unschedule);
PG_FUNCTION_INFO_V1(cron_unschedule_named);
PG_FUNCTION_INFO_V1(cron_job_cache_invalidate);
PG_FUNCTION_INFO_V1(cron_alter_job);
PG_FUNCTION_INFO_V1(cron_schedule_named_mode);
PG_FUNCTION_INFO_V1(cron_schedule_named_mode_zone);


/* global variables */
static MemoryContext CronJobContext = NULL;
static HTAB *CronJobHash = NULL;
static Oid CachedCronJobRelationId = InvalidOid;
bool CronJobCacheValid = false;
char *CronHost = "localhost";
bool EnableSuperuserJobs = true;


/*
 * InitializeJobMetadataCache initializes the data structures for caching
 * job metadata.
 */
void
InitializeJobMetadataCache(void)
{
	/* watch for invalidation events */
	CacheRegisterRelcacheCallback(InvalidateJobCacheCallback, (Datum) 0);

	CronJobContext = AllocSetContextCreate(CurrentMemoryContext,
											 "pg_cron job context",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);

	CronJobHash = CreateCronJobHash();
}


/*
 * ResetJobMetadataCache resets the job metadata cache to its initial
 * state.
 */
void
ResetJobMetadataCache(void)
{
	MemoryContextResetAndDeleteChildren(CronJobContext);

	CronJobHash = CreateCronJobHash();
}


/*
 * CreateCronJobHash creates the hash for caching job metadata.
 */
static HTAB *
CreateCronJobHash(void)
{
	HTAB *taskHash = NULL;
	HASHCTL info;
	int hashFlags = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(CronJob);
	info.hash = tag_hash;
	info.hcxt = CronJobContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	taskHash = hash_create("pg_cron jobs", 32, &info, hashFlags);

	return taskHash;
}


/*
 * GetCronJob gets the cron job with the given id.
 */
CronJob *
GetCronJob(int64 jobId)
{
	CronJob *job = NULL;
	int64 hashKey = jobId;
	bool isPresent = false;

	job = hash_search(CronJobHash, &hashKey, HASH_FIND, &isPresent);

	return job;
}

/*
 * ScheduleCronJob schedules a cron job with the given name/mode/zone.
 */
static int64
ScheduleCronJob(text *scheduleText, text *commandText, text *databaseText,
					text *usernameText, bool active, text *jobnameText, char *mode, char *tmzone)
{
	entry *parsedSchedule = NULL;
	char *schedule;
	char *command;
	char *database_name;
	char *jobName = NULL;
	char *username;
	AclResult aclresult;
	Oid userIdcheckacl;

	int64 jobId = 0;
	Datum jobIdDatum = 0;

	StringInfoData querybuf;
	Oid argTypes[8];
	Datum argValues[8];
	int argCount = 0;

	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;

	TupleDesc returnedRowDescriptor = NULL;
	HeapTuple returnedRow = NULL;
	bool returnedJobIdIsNull = false;

	Oid userId = GetUserId();
	userIdcheckacl = GetUserId();
	username = GetUserNameFromId(userId, false);

	/* check schedule is valid */
	schedule = text_to_cstring(scheduleText);
	parsedSchedule = parse_cron_entry(schedule);

	if (parsedSchedule == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid schedule: %s", schedule)));
	}

	free_entry(parsedSchedule);

	if (strcmp(MODE_SINGLE, mode) && strcmp(MODE_TIMING, mode))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid mode: %s", mode)));
	}


	if (12 < atoi(tmzone) || -12 > atoi(tmzone))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid timezone: %s", tmzone)));
	}

	initStringInfo(&querybuf);

	appendStringInfo(&querybuf,
		"insert into %s (schedule, command, nodename, nodeport, database, username, active",
		quote_qualified_identifier(CRON_SCHEMA_NAME, JOBS_TABLE_NAME));

	if (jobnameText != NULL)
	{
		appendStringInfo(&querybuf, ", jobname");
	}

	appendStringInfo(&querybuf, ") values ($1, $2, $3, $4, $5, $6, $7");

	if (jobnameText != NULL)
	{
		appendStringInfo(&querybuf, ", $8) ");
		appendStringInfo(&querybuf, "on conflict on constraint jobname_username_uniq ");
		appendStringInfo(&querybuf, "do update set ");
		appendStringInfo(&querybuf, "schedule = EXCLUDED.schedule, ");
		appendStringInfo(&querybuf, "command = EXCLUDED.command");
	}
	else
	{
		appendStringInfo(&querybuf, ")");
	}

	appendStringInfo(&querybuf, " returning jobid");

	argTypes[0] = TEXTOID;
	argValues[0] = CStringGetTextDatum(schedule);
	argCount++;

	argTypes[1] = TEXTOID;
	command = text_to_cstring(commandText);
	argValues[1] = CStringGetTextDatum(command);
	argCount++;

	argTypes[2] = TEXTOID;
	argValues[2] = CStringGetTextDatum(CronHost);
	argCount++;

	argTypes[3] = INT4OID;
	argValues[3] = Int32GetDatum(PostPortNumber);
	argCount++;

	/* username has been provided */
	if (usernameText != NULL)
	{
		if (!superuser())
			elog(ERROR, "must be superuser to create a job for another role");

		username = text_to_cstring(usernameText);
		userIdcheckacl = GetRoleOidIfCanLogin(username);
	}

	/* database has been provided */
	if (databaseText != NULL)
		database_name = text_to_cstring(databaseText);
	else
	/* use the GUC */
		database_name = CronTableDatabaseName;

	/* first do a crude check to see whether superuser jobs are allowed */
	if (!EnableSuperuserJobs && superuser_arg(userIdcheckacl))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cannot schedule jobs as superuser"),
						errdetail("Scheduling jobs as superuser is disallowed when "
								  "cron.enable_superuser_jobs is set to off.")));
	}

	/* ensure the user that is used in the job can connect to the database */
	aclresult = pg_database_aclcheck(get_database_oid(database_name, false),
										userIdcheckacl, ACL_CONNECT);

	if (aclresult != ACLCHECK_OK)
		elog(ERROR, "User %s does not have CONNECT privilege on %s",
				GetUserNameFromId(userIdcheckacl, false), database_name);

	argTypes[4] = TEXTOID;
	argValues[4] = CStringGetTextDatum(database_name);
	argCount++;

	argTypes[5] = TEXTOID;
	argValues[5] = CStringGetTextDatum(username);
	argCount++;

	argTypes[6] = BOOLOID;
	argValues[6] = BoolGetDatum(active);
	argCount++;

	if (jobnameText != NULL)
	{
		argTypes[7] = TEXTOID;
		jobName = text_to_cstring(jobnameText);
		argValues[7] = CStringGetTextDatum(jobName);
		argCount++;
	}

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CronExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect failed");
	}

	if (SPI_execute_with_args(querybuf.data, argCount, argTypes, argValues, NULL,
							  false, 1) != SPI_OK_INSERT_RETURNING)
	{
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	}

	if (SPI_processed <= 0)
	{
		elog(ERROR, "query did not return any rows: %s", querybuf.data);
	}

	returnedRowDescriptor = SPI_tuptable->tupdesc;
	returnedRow = SPI_tuptable->vals[0];

	jobIdDatum = SPI_getbinval(returnedRow, returnedRowDescriptor, 1,
							   &returnedJobIdIsNull);
	jobId = DatumGetInt64(jobIdDatum);

	pfree(querybuf.data);

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	InvalidateJobCache();

	insertCronExt(jobId, (char *)jobName, mode, tmzone);

	return jobId;
}

/*
 * GetRoleOidIfCanLogin
 * Checks user exist and can log in
 */
static Oid
GetRoleOidIfCanLogin(char *username)
{
	HeapTuple   roletup;
	Form_pg_authid rform;
	Oid roleOid = InvalidOid;

	roletup = SearchSysCache1(AUTHNAME, PointerGetDatum(username));
	if (!HeapTupleIsValid(roletup))
		ereport(ERROR,
				(errmsg("role \"%s\" does not exist",
						username)));

	rform = (Form_pg_authid) GETSTRUCT(roletup);

	if (!rform->rolcanlogin)
		ereport(ERROR,
				(errmsg("role \"%s\" can not log in",
						username),
				 errdetail("Jobs may only be run by roles that have the LOGIN attribute.")));

#if (PG_VERSION_NUM < 120000)
	roleOid = HeapTupleGetOid(roletup);
#else
	roleOid = rform->oid;
#endif

	ReleaseSysCache(roletup);
	return roleOid;
}

/*
 * cron_alter_job alter a job
 */
Datum
cron_alter_job(PG_FUNCTION_ARGS)
{
	int64 jobId;
	text *scheduleText = NULL;
	text *commandText = NULL;
	text *databaseText = NULL;
	text *usernameText = NULL;
	bool active;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errmsg("job_id can not be NULL")));
	else
		jobId = PG_GETARG_INT64(0);

	if (!PG_ARGISNULL(1))
		scheduleText = PG_GETARG_TEXT_P(1);

	if (!PG_ARGISNULL(2))
		commandText = PG_GETARG_TEXT_P(2);

	if (!PG_ARGISNULL(3))
		databaseText = PG_GETARG_TEXT_P(3);

	if (!PG_ARGISNULL(4))
		usernameText = PG_GETARG_TEXT_P(4);

	if (!PG_ARGISNULL(5))
		active = PG_GETARG_BOOL(5);

	AlterJob(jobId, scheduleText, commandText, databaseText, usernameText,
				PG_ARGISNULL(5) ? NULL : &active);

	PG_RETURN_VOID();
}


/*
 * cron_schedule schedule a job
 */
Datum
cron_schedule(PG_FUNCTION_ARGS)
{
	text *scheduleText = NULL;
	text *commandText = NULL;
	int64 jobId;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errmsg("schedule can not be NULL")));
	else
		scheduleText = PG_GETARG_TEXT_P(0);

	if (PG_ARGISNULL(1))
		ereport(ERROR, (errmsg("command can not be NULL")));
	else
		commandText = PG_GETARG_TEXT_P(1);

	jobId = ScheduleCronJob(scheduleText, commandText, NULL,
							NULL, true, NULL, MODE_TIMING, DEFAULT_TIME_ZONE);

	PG_RETURN_INT64(jobId);
}

/*
 * cron_schedule schedule a named job
 */
Datum
cron_schedule_named(PG_FUNCTION_ARGS)
{
	text *scheduleText = NULL;
	text *commandText = NULL;
	text *databaseText = NULL;
	text *usernameText = NULL;
	bool active = true;
	text *jobnameText = NULL;
	int64 jobId;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errmsg("job_name can not be NULL")));
	else
		jobnameText = PG_GETARG_TEXT_P(0);

	if (PG_ARGISNULL(1))
		ereport(ERROR, (errmsg("schedule can not be NULL")));
	else
		scheduleText = PG_GETARG_TEXT_P(1);

	if (PG_ARGISNULL(2))
		ereport(ERROR, (errmsg("command can not be NULL")));
	else
		commandText = PG_GETARG_TEXT_P(2);

	if (PG_NARGS() > 3)
	{
		if (!PG_ARGISNULL(3))
			databaseText = PG_GETARG_TEXT_P(3);

		if (!PG_ARGISNULL(4))
			usernameText = PG_GETARG_TEXT_P(4);

		if (!PG_ARGISNULL(5))
			active = PG_GETARG_BOOL(5);
	}

	jobId = ScheduleCronJob(scheduleText, commandText, databaseText,
							usernameText, active, jobnameText, MODE_TIMING, DEFAULT_TIME_ZONE);

	PG_RETURN_INT64(jobId);
}

/*
 * cron_schedule_named_mode schedules a mode cron job
 */
Datum
cron_schedule_named_mode(PG_FUNCTION_ARGS)
{
	text *jobnameText = PG_GETARG_TEXT_P(0);
	text *scheduleText = PG_GETARG_TEXT_P(1);
	text *commandText = PG_GETARG_TEXT_P(2);
	text *modeText = PG_GETARG_TEXT_P(3);

	char *mode = text_to_cstring(modeText);

	int64 jobId = ScheduleCronJob(scheduleText, commandText, NULL, NULL, true, jobnameText, mode, DEFAULT_TIME_ZONE);

	PG_RETURN_INT64(jobId);
}

/*
 * cron_schedule_named_mode_zone schedules a zone cron job
 */
Datum
cron_schedule_named_mode_zone(PG_FUNCTION_ARGS)
{
	text *jobnameText = PG_GETARG_TEXT_P(0);
	text *scheduleText = PG_GETARG_TEXT_P(1);
	text *commandText = PG_GETARG_TEXT_P(2);
	text *modeText = PG_GETARG_TEXT_P(3);
	text *zoneText = PG_GETARG_TEXT_P(4);

	char *mode = text_to_cstring(modeText);
	char *tmzone = text_to_cstring(zoneText);

	int64 jobId = ScheduleCronJob(scheduleText, commandText, NULL, NULL, true, jobnameText, mode, tmzone);

	PG_RETURN_INT64(jobId);
}

/*
 * NextRunId draws a new run ID from cron.runid_seq.
 */
int64
NextRunId(void)
{
	text *sequenceName = NULL;
	Oid sequenceId = InvalidOid;
	List *sequenceNameList = NIL;
	RangeVar *sequenceVar = NULL;
	Datum sequenceIdDatum = InvalidOid;
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum jobIdDatum = 0;
	int64 jobId = 0;
	bool failOK = true;
	MemoryContext originalContext = CurrentMemoryContext;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (!JobRunDetailsTableExists())
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(originalContext);

		/* if the job_run_details table is not yet created, the run ID is not used */
		return 0;
	}

	/* resolve relationId from passed in schema and relation name */
	sequenceName = cstring_to_text(RUN_ID_SEQUENCE_NAME);
	sequenceNameList = textToQualifiedNameList(sequenceName);
	sequenceVar = makeRangeVarFromNameList(sequenceNameList);
	sequenceId = RangeVarGetRelid(sequenceVar, NoLock, failOK);
	sequenceIdDatum = ObjectIdGetDatum(sequenceId);

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CronExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique colocation id from sequence */
	jobIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	jobId = DatumGetInt64(jobIdDatum);

	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);

	return jobId;
}

/*
 * CronExtensionOwner returns the name of the user that owns the
 * extension.
 */
static Oid
CronExtensionOwner(void)
{
	Relation extensionRelation = NULL;
	SysScanDesc scanDescriptor;
	ScanKeyData entry[1];
	HeapTuple extensionTuple = NULL;
	Form_pg_extension extensionForm = NULL;
	Oid extensionOwner = InvalidOid;

	extensionRelation = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(EXTENSION_NAME));

	scanDescriptor = systable_beginscan(extensionRelation, ExtensionNameIndexId,
										true, NULL, 1, entry);

	extensionTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(extensionTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("pg_cron extension not loaded")));
	}

	extensionForm = (Form_pg_extension) GETSTRUCT(extensionTuple);
	extensionOwner = extensionForm->extowner;

	systable_endscan(scanDescriptor);
	table_close(extensionRelation, AccessShareLock);

	return extensionOwner;
}


/*
 * cron_unschedule removes a cron job.
 */
Datum
cron_unschedule(PG_FUNCTION_ARGS)
{
	int64 jobId = PG_GETARG_INT64(0);

	Oid cronSchemaId = InvalidOid;
	Oid cronJobIndexId = InvalidOid;

	Relation cronJobsTable = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	HeapTuple heapTuple = NULL;

	cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
	cronJobIndexId = get_relname_relid(JOB_ID_INDEX_NAME, cronSchemaId);

	cronJobsTable = table_open(CronJobRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_cron_job_jobid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

	scanDescriptor = systable_beginscan(cronJobsTable,
										cronJobIndexId, indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for job "
							   INT64_FORMAT, jobId)));
	}

	EnsureDeletePermission(cronJobsTable, heapTuple);

	simple_heap_delete(cronJobsTable, &heapTuple->t_self);

	systable_endscan(scanDescriptor);
	table_close(cronJobsTable, NoLock);

	CommandCounterIncrement();
	InvalidateJobCache();

    deleteCronExt(jobId, NULL);

	PG_RETURN_BOOL(true);
}


/*
 * cron_unschedule_named removes a cron job by name.
 */
Datum
cron_unschedule_named(PG_FUNCTION_ARGS)
{
	Datum jobNameDatum = PG_GETARG_DATUM(0);
	Name jobName = DatumGetName(jobNameDatum);

	Oid userId = GetUserId();
	char *userName = GetUserNameFromId(userId, false);
	Datum userNameDatum = CStringGetTextDatum(userName);

	Relation cronJobsTable = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[2];
	int scanKeyCount = 2;
	bool indexOK = false;
	HeapTuple heapTuple = NULL;

	cronJobsTable = table_open(CronJobRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_cron_job_jobname,
				BTEqualStrategyNumber, F_NAMEEQ, jobNameDatum);
	ScanKeyInit(&scanKey[1], Anum_cron_job_username,
				BTEqualStrategyNumber, F_TEXTEQ, userNameDatum);

	scanDescriptor = systable_beginscan(cronJobsTable, InvalidOid, indexOK,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for job '%s'",
							   NameStr(*jobName))));
	}

	EnsureDeletePermission(cronJobsTable, heapTuple);

	simple_heap_delete(cronJobsTable, &heapTuple->t_self);

	systable_endscan(scanDescriptor);
	table_close(cronJobsTable, NoLock);

	CommandCounterIncrement();
	InvalidateJobCache();

    deleteCronExt(0, (char *)jobName);

	PG_RETURN_BOOL(true);
}


/*
 * EnsureDeletePermission throws an error if the current user does
 * not have permission to delete the given cron.job tuple.
 */
static void
EnsureDeletePermission(Relation cronJobsTable, HeapTuple heapTuple)
{
	TupleDesc tupleDescriptor = RelationGetDescr(cronJobsTable);

	/* check if the current user owns the row */
	Oid userId = GetUserId();
	char *userName = GetUserNameFromId(userId, false);

	bool isNull = false;
	Datum ownerNameDatum = heap_getattr(heapTuple, Anum_cron_job_username,
										tupleDescriptor, &isNull);
	char *ownerName = TextDatumGetCString(ownerNameDatum);
	if (pg_strcasecmp(userName, ownerName) != 0)
	{
		/* otherwise, allow if the user has DELETE permission */
		AclResult aclResult = pg_class_aclcheck(CronJobRelationId(), GetUserId(),
												ACL_DELETE);
		if (aclResult != ACLCHECK_OK)
		{
			aclcheck_error(aclResult,
#if (PG_VERSION_NUM < 110000)
						   ACL_KIND_CLASS,
#else
						   OBJECT_TABLE,
#endif
						   get_rel_name(CronJobRelationId()));
		}
	}
}


/*
 * cron_job_cache_invalidate invalidates the job cache in response to
 * a trigger.
 */
Datum
cron_job_cache_invalidate(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	InvalidateJobCache();

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * Invalidate job cache ensures the job cache is reloaded on the next
 * iteration of pg_cron.
 */
static void
InvalidateJobCache(void)
{
	HeapTuple classTuple = NULL;

	classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(CronJobRelationId()));
	if (HeapTupleIsValid(classTuple))
	{
		CacheInvalidateRelcacheByTuple(classTuple);
		ReleaseSysCache(classTuple);
	}
}


/*
 * InvalidateJobCacheCallback invalidates the job cache in response to
 * an invalidation event.
 */
static void
InvalidateJobCacheCallback(Datum argument, Oid relationId)
{
	if (relationId == CachedCronJobRelationId ||
		CachedCronJobRelationId == InvalidOid)
	{
		CronJobCacheValid = false;
		CachedCronJobRelationId = InvalidOid;
	}
}


/*
 * CachedCronJobRelationId returns a cached oid of the cron.job relation.
 */
static Oid
CronJobRelationId(void)
{
	if (CachedCronJobRelationId == InvalidOid)
	{
		Oid cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);

		CachedCronJobRelationId = get_relname_relid(JOBS_TABLE_NAME, cronSchemaId);
	}

	return CachedCronJobRelationId;
}

/*
 * LoadCronJobList loads the current list of jobs from the
 * cron.job table and adds each job to the CronJobHash.
 */
List *
LoadCronJobList(void)
{
	List *jobList = NIL;

	Relation cronJobTable = NULL;

	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	MemoryContext originalContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * If the pg_cron extension has not been created yet or
	 * we are on a hot standby, the job table is treated as
	 * being empty.
	 */
	if (!PgCronHasBeenLoaded() || RecoveryInProgress())
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(originalContext);
		pgstat_report_activity(STATE_IDLE, NULL);

		return NIL;
	}

	cronJobTable = table_open(CronJobRelationId(), AccessShareLock);

	scanDescriptor = systable_beginscan(cronJobTable,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(cronJobTable);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		MemoryContext oldContext = NULL;
		CronJob *job = NULL;
		Oid jobOwnerId = InvalidOid;

		oldContext = MemoryContextSwitchTo(CronJobContext);

		job = TupleToCronJob(tupleDescriptor, heapTuple);

		jobOwnerId = get_role_oid(job->userName, false);
		if (!EnableSuperuserJobs && superuser_arg(jobOwnerId))
		{
			/*
			 * Someone inserted a superuser into the metadata. Skip over the
			 * job when cron.enable_superuser_jobs is disabled. The memory
			 * will be cleaned up when CronJobContext is reset.
			 */
			ereport(WARNING, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							  errmsg("skipping job " INT64_FORMAT " since superuser jobs "
									 "are currently disallowed",
									 job->jobId)));
		}
		else
		{
			jobList = lappend(jobList, job);
		}

		MemoryContextSwitchTo(oldContext);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(cronJobTable, AccessShareLock);

	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);
	pgstat_report_activity(STATE_IDLE, NULL);

	return jobList;
}


/*
 * TupleToCronJob takes a heap tuple and converts it into a CronJob
 * struct.
 */
static CronJob *
TupleToCronJob(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	CronJob *job = NULL;
	int64 jobKey = 0;
	bool isNull = false;
	bool isPresent = false;
	entry *parsedSchedule = NULL;

	Datum jobId = heap_getattr(heapTuple, Anum_cron_job_jobid,
							   tupleDescriptor, &isNull);
	Datum schedule = heap_getattr(heapTuple, Anum_cron_job_schedule,
								  tupleDescriptor, &isNull);
	Datum command = heap_getattr(heapTuple, Anum_cron_job_command,
								 tupleDescriptor, &isNull);
	Datum nodeName = heap_getattr(heapTuple, Anum_cron_job_nodename,
								  tupleDescriptor, &isNull);
	Datum nodePort = heap_getattr(heapTuple, Anum_cron_job_nodeport,
								  tupleDescriptor, &isNull);
	Datum database = heap_getattr(heapTuple, Anum_cron_job_database,
								  tupleDescriptor, &isNull);
	Datum userName = heap_getattr(heapTuple, Anum_cron_job_username,
								  tupleDescriptor, &isNull);

	jobKey = DatumGetInt64(jobId);
	job = hash_search(CronJobHash, &jobKey, HASH_ENTER, &isPresent);

	job->jobId = DatumGetInt64(jobId);
	job->scheduleText = TextDatumGetCString(schedule);
	job->command = TextDatumGetCString(command);
	job->nodeName = TextDatumGetCString(nodeName);
	job->nodePort = DatumGetInt32(nodePort);
	job->userName = TextDatumGetCString(userName);
	job->database = TextDatumGetCString(database);

	if (HeapTupleHeaderGetNatts(heapTuple->t_data) >= Anum_cron_job_active)
	{
		Datum active = heap_getattr(heapTuple, Anum_cron_job_active,
								tupleDescriptor, &isNull);
		Assert(!isNull);
		job->active = DatumGetBool(active);
	}
	else
	{
		job->active = true;
	}

	if (tupleDescriptor->natts >= Anum_cron_job_jobname)
	{
		bool isJobNameNull = false;
		Datum jobName = heap_getattr(heapTuple, Anum_cron_job_jobname,
									 tupleDescriptor, &isJobNameNull);
		if (!isJobNameNull)
		{
			job->jobName = DatumGetName(jobName);
		}
		else
		{
			job->jobName = NULL;
		}
	}

	parsedSchedule = parse_cron_entry(job->scheduleText);
	if (parsedSchedule != NULL)
	{
		/* copy the schedule and free the allocated memory immediately */

		job->schedule = *parsedSchedule;
		free_entry(parsedSchedule);
	}
	else
	{
		ereport(LOG, (errmsg("invalid pg_cron schedule for job " INT64_FORMAT ": %s",
							 job->jobId, job->scheduleText)));

		/* a zeroed out schedule never runs */
		memset(&job->schedule, 0, sizeof(entry));
	}

	return job;
}


/*
 * PgCronHasBeenLoaded returns true if the pg_cron extension has been created
 * in the current database and the extension script has been executed. Otherwise,
 * it returns false. The result is cached as this is called very frequently.
 */
static bool
PgCronHasBeenLoaded(void)
{
	bool extensionLoaded = false;
	bool extensionPresent = false;
	bool extensionScriptExecuted = true;

	Oid extensionOid = get_extension_oid(EXTENSION_NAME, true);
	if (extensionOid != InvalidOid)
	{
		extensionPresent = true;
	}

	if (extensionPresent)
	{
		/* check if pg_cron extension objects are still being created */
		if (creating_extension && CurrentExtensionObject == extensionOid)
		{
			extensionScriptExecuted = false;
		}
		else if (IsBinaryUpgrade)
		{
			extensionScriptExecuted = false;
		}
	}

	extensionLoaded = extensionPresent && extensionScriptExecuted;

	return extensionLoaded;
}

void
InsertJobRunDetail(int64 runId, int64 *jobId, char *database, char *username, char *command, char *status)
{
	StringInfoData querybuf;
	const int argCount = 6;
	Oid argTypes[6];
	Datum argValues[6];
	MemoryContext originalContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (!PgCronHasBeenLoaded() || RecoveryInProgress() || !JobRunDetailsTableExists())
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(originalContext);
		return;
	}

	initStringInfo(&querybuf);

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");


	appendStringInfo(&querybuf,
		"insert into %s.%s (jobid, runid, database, username, command, status) values ($1,$2,$3,$4,$5,$6)",
		CRON_SCHEMA_NAME, JOB_RUN_DETAILS_TABLE_NAME);

	/* jobId */
	argTypes[0] = INT8OID;
	argValues[0] = Int64GetDatum(*jobId);

	/* runId */
	argTypes[1] = INT8OID;
	argValues[1] = Int64GetDatum(runId);

	/* database */
	argTypes[2] = TEXTOID;
	argValues[2] = CStringGetTextDatum(database);

	/* username */
	argTypes[3] = TEXTOID;
	argValues[3] = CStringGetTextDatum(username);

	/* command */
	argTypes[4] = TEXTOID;
	argValues[4] = CStringGetTextDatum(command);

	/* status */
	argTypes[5] = TEXTOID;
	argValues[5] = CStringGetTextDatum(status);

	pgstat_report_activity(STATE_RUNNING, querybuf.data);

	if(SPI_execute_with_args(querybuf.data,
		argCount, argTypes, argValues, NULL, false, 1) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	pfree(querybuf.data);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
UpdateJobRunDetail(int64 runId, int32 *job_pid, char *status, char *return_message, TimestampTz *start_time,
                                                                        TimestampTz *end_time)
{
	StringInfoData querybuf;
	Oid argTypes[6];
	Datum argValues[6];
	int i;
	MemoryContext originalContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (!PgCronHasBeenLoaded() || RecoveryInProgress() || !JobRunDetailsTableExists())
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(originalContext);
		return;
	}

	initStringInfo(&querybuf);
	i = 0;

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");


	appendStringInfo(&querybuf,
		"update %s.%s set", CRON_SCHEMA_NAME, JOB_RUN_DETAILS_TABLE_NAME);


	/* add the fields to be updated */
	if (job_pid != NULL) {
		argTypes[i] = INT4OID;
		argValues[i] = Int32GetDatum(*job_pid);
		i++;
		appendStringInfo(&querybuf, " job_pid = $%d,", i);
	}

	if (status != NULL)
	{
		argTypes[i] = TEXTOID;
		argValues[i] = CStringGetTextDatum(status);
		i++;

		appendStringInfo(&querybuf, " status = $%d,", i);
	}

        if (return_message != NULL)
	{
		argTypes[i] = TEXTOID;
		argValues[i] = CStringGetTextDatum(return_message);
		i++;

		appendStringInfo(&querybuf, " return_message = $%d,", i);
	}

        if (start_time != NULL)
	{
		argTypes[i] = TIMESTAMPTZOID;
		argValues[i] = TimestampTzGetDatum(*start_time);
		i++;

		appendStringInfo(&querybuf, " start_time = $%d,", i);
	}

        if (end_time != NULL)
	{
		argTypes[i] = TIMESTAMPTZOID;
		argValues[i] = TimestampTzGetDatum(*end_time);
		i++;

		appendStringInfo(&querybuf, " end_time = $%d,", i);
	}

	argTypes[i] = INT8OID;
	argValues[i] = Int64GetDatum(runId);
	i++;

	/* remove the last comma */
	querybuf.len--;
	querybuf.data[querybuf.len] = '\0';

	/* and add the where clause */
	appendStringInfo(&querybuf, " where runid = $%d", i);

	pgstat_report_activity(STATE_RUNNING, querybuf.data);

	if(SPI_execute_with_args(querybuf.data,
		i, argTypes, argValues, NULL, false, 1) != SPI_OK_UPDATE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	pfree(querybuf.data);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);
	pgstat_report_activity(STATE_IDLE, NULL);
}


static void
AlterJob(int64 jobId, text *scheduleText, text *commandText, text *databaseText, text *usernameText, bool *active)
{
	StringInfoData querybuf;
	Oid argTypes[7];
	Datum argValues[7];
	int i;
	AclResult aclresult;
	Oid userId;
	Oid userIdcheckacl;
	Oid savedUserId;
	int savedSecurityContext;
	char *database_name;
	char *schedule;
	char *command;
	char *username;
	char *currentuser;
	entry *parsedSchedule = NULL;

	userId = GetUserId();
	userIdcheckacl = GetUserId();

	currentuser = GetUserNameFromId(userId, false);
	savedUserId = InvalidOid;
	savedSecurityContext = 0;

	if (!PgCronHasBeenLoaded() || RecoveryInProgress() || !JobTableExists())
	{
		return;
	}

	initStringInfo(&querybuf);
	i = 0;

	appendStringInfo(&querybuf,
		"update %s.%s set", CRON_SCHEMA_NAME, JOBS_TABLE_NAME);

	/* username has been provided */
	if (usernameText != NULL)
	{
		if (!superuser())
			elog(ERROR, "must be superuser to alter username");

		username = text_to_cstring(usernameText);
		userIdcheckacl = GetRoleOidIfCanLogin(username);
	}
	else
	{
		username = currentuser;
	}

	if (!EnableSuperuserJobs && superuser_arg(userIdcheckacl))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cannot schedule jobs as superuser"),
						errdetail("Scheduling jobs as superuser is disallowed when "
								  "cron.enable_superuser_jobs is set to off.")));
	}

	/* add the fields to be updated */
	/* database has been provided */
	if (databaseText != NULL)
	{
		database_name = text_to_cstring(databaseText);
		/* ensure the user that is used in the job can connect to the database */
		aclresult = pg_database_aclcheck(get_database_oid(database_name, false), userIdcheckacl, ACL_CONNECT);

		if (aclresult != ACLCHECK_OK)
			elog(ERROR, "User %s does not have CONNECT privilege on %s", GetUserNameFromId(userIdcheckacl, false), database_name);

		argTypes[i] = TEXTOID;
		argValues[i] = CStringGetTextDatum(database_name);
		i++;
		appendStringInfo(&querybuf, " database = $%d,", i);
	}

	/* ensure schedule is valid */
	if (scheduleText != NULL)
	{
		schedule = text_to_cstring(scheduleText);
		parsedSchedule = parse_cron_entry(schedule);

		if (parsedSchedule == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("invalid schedule: %s", schedule)));
		}

		free_entry(parsedSchedule);

		argTypes[i] = TEXTOID;
		argValues[i] = CStringGetTextDatum(schedule);
		i++;
		appendStringInfo(&querybuf, " schedule = $%d,", i);
	}

	if (commandText != NULL)
	{
		argTypes[i] = TEXTOID;
		command = text_to_cstring(commandText);
		argValues[i] = CStringGetTextDatum(command);
		i++;
		appendStringInfo(&querybuf, " command = $%d,", i);
	}

	if (usernameText != NULL)
	{
		argTypes[i] = TEXTOID;
		argValues[i] = CStringGetTextDatum(username);
		i++;
		appendStringInfo(&querybuf, " username = $%d,", i);
	}

	if (active != NULL)
	{
		argTypes[i] = BOOLOID;
		argValues[i] = BoolGetDatum(*active);
		i++;
		appendStringInfo(&querybuf, " active = $%d,", i);
	}

	/* remove the last comma */
	querybuf.len--;
	querybuf.data[querybuf.len] = '\0';

	/* and add the where clause */
	argTypes[i] = INT8OID;
	argValues[i] = Int64GetDatum(jobId);
	i++;

	appendStringInfo(&querybuf, " where jobid = $%d", i);

	/* ensure the caller owns the row */
	argTypes[i] = TEXTOID;
	argValues[i] = CStringGetTextDatum(currentuser);
	i++;

	if (!superuser())
		appendStringInfo(&querybuf, " and username = $%d", i);

	if (i <= 2)
		ereport(ERROR, (errmsg("no updates specified"),
						errhint("You must specify at least one job attribute to change when calling alter_job")));

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CronExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	if(SPI_execute_with_args(querybuf.data,
		i, argTypes, argValues, NULL, false, 1) != SPI_OK_UPDATE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	pfree(querybuf.data);

	if (SPI_processed <= 0)
		elog(ERROR, "Job " INT64_FORMAT " does not exist or you don't own it", jobId);

	SPI_finish();
	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
	InvalidateJobCache();
}

void
MarkPendingRunsAsFailed(void)
{
	StringInfoData querybuf;
	MemoryContext originalContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (!PgCronHasBeenLoaded() || RecoveryInProgress() || !JobRunDetailsTableExists())
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(originalContext);
		return;
	}

	initStringInfo(&querybuf);

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");


	appendStringInfo(&querybuf,
		"update %s.%s set status = '%s', return_message = 'server restarted' where status in ('%s','%s')"
		, CRON_SCHEMA_NAME, JOB_RUN_DETAILS_TABLE_NAME, GetCronStatus(CRON_STATUS_FAILED), GetCronStatus(CRON_STATUS_STARTING), GetCronStatus(CRON_STATUS_RUNNING));


	pgstat_report_activity(STATE_RUNNING, querybuf.data);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_UPDATE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	pfree(querybuf.data);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);
	pgstat_report_activity(STATE_IDLE, NULL);
}

char *
GetCronStatus(CronStatus cronstatus)
{
	char *statusDesc = "unknown status";

	switch (cronstatus)
	{
	case CRON_STATUS_STARTING:
		statusDesc = "starting";
		break;
	case CRON_STATUS_RUNNING:
		statusDesc = "running";
		break;
	case CRON_STATUS_SENDING:
		statusDesc = "sending";
		break;
	case CRON_STATUS_CONNECTING:
		statusDesc = "connecting";
		break;
	case CRON_STATUS_SUCCEEDED:
		statusDesc = "succeeded";
		break;
	case CRON_STATUS_FAILED:
		statusDesc = "failed";
		break;
	default:
		break;
	}
	return statusDesc;
}


/*
 * JobRunDetailsTableExists returns whether the job_run_details table exists.
 */
static bool
JobRunDetailsTableExists(void)
{
	Oid cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
	Oid jobRunDetailsTableOid = get_relname_relid(JOB_RUN_DETAILS_TABLE_NAME,
												  cronSchemaId);

	return jobRunDetailsTableOid != InvalidOid;
}

/*
 * JobTableExists returns whether the job table exists.
 */
static bool
JobTableExists(void)
{
	Oid cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
	Oid jobTableOid = get_relname_relid(JOBS_TABLE_NAME,
										cronSchemaId);

	return jobTableOid != InvalidOid;
}

/*
 * JobLtExtTableExists returns whether the lt_job_ext table exists.
 */
static bool
JobLtExtTableExists(void)
{
    Oid cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
    Oid jobLtExtTableOid = get_relname_relid(LT_JOB_EXT,
                                                  cronSchemaId);

    return jobLtExtTableOid != InvalidOid;
}

/*
 * insert data into cron.lt_job_ext
 */
static void
insertCronExt(int64 jobid, char *jobname, char *mode, char *tmzone)
{
    char updatebuf[1024] = {0};
    Oid userId = GetUserId();
	char *userName = GetUserNameFromId(userId, false);

	if(!JobLtExtTableExists())
	{
		return;
	}

    /* Open SPI context. */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(ERROR, "SPI_connect failed");
    }

	if (NULL == jobname)
    {
        snprintf(updatebuf, 1024,
         "insert into %s (jobid, username, mode, timezone) values (%lld, '%s', '%s', %s) on conflict on constraint jobid_username_uniq do update set mode = EXCLUDED.mode, timezone = EXCLUDED.timezone",
         quote_qualified_identifier(CRON_SCHEMA_NAME, LT_JOB_EXT), (long long int)jobid, userName, mode, tmzone);
    }
	else
	{
		snprintf(updatebuf, 1024,
         "insert into %s (jobid, jobname, username, mode, timezone) values (%lld, '%s', '%s', '%s', %s) on conflict on constraint jobid_username_uniq do update set mode = EXCLUDED.mode, timezone = EXCLUDED.timezone",
         quote_qualified_identifier(CRON_SCHEMA_NAME, LT_JOB_EXT), (long long int)jobid, jobname, userName, mode, tmzone);
	}

    SPI_execute(updatebuf, false, 0);

    memset(updatebuf, 0, 1024);
    snprintf(updatebuf, 1024,
             "update %s set active = 'true' where jobid = %lld",
             quote_qualified_identifier(CRON_SCHEMA_NAME, JOBS_TABLE_NAME), (long long int)jobid);

    SPI_execute(updatebuf, false, 0);

    SPI_finish();
}

/*
 * delete data from cron.lt_job_ext where == jobid
 */
static void
deleteCronExt(int64 jobid, char *jobname)
{
    char deletebuf[1024] = {0};

	if(!JobLtExtTableExists())
	{
		return;
	}

    if (NULL != jobname)
    {
        snprintf(deletebuf, 1024,
             "delete from %s where jobname = '%s'",
             quote_qualified_identifier(CRON_SCHEMA_NAME, LT_JOB_EXT), jobname);
    }
    else
    {
        snprintf(deletebuf, 1024,
             "delete from %s where jobid = %lld",
             quote_qualified_identifier(CRON_SCHEMA_NAME, LT_JOB_EXT), (long long int)jobid);
    }

    /* Open SPI context. */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(ERROR, "SPI_connect failed");
    }

    SPI_execute(deletebuf, false, 0);

    SPI_finish();
}

/*
 * query mode from cron.lt_job_ext where == jobid
 */
void
queryModeFromCronExt(int64 jobid, char *mode)
{
    StringInfoData querybuf;
    MemoryContext originalContext = CurrentMemoryContext;
    char *buf = NULL;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    if (!PgCronHasBeenLoaded() || RecoveryInProgress() || !JobLtExtTableExists())
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
        MemoryContextSwitchTo(originalContext);
        return;
    }

    initStringInfo(&querybuf);

    /* Open SPI context. */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(ERROR, "SPI_connect failed");
    }

    appendStringInfo(&querybuf, "select mode from %s where jobid = %lld",
             quote_qualified_identifier(CRON_SCHEMA_NAME, LT_JOB_EXT), (long long int)jobid);

    pgstat_report_activity(STATE_RUNNING, querybuf.data);

    if (SPI_exec(querybuf.data, 0) != SPI_OK_SELECT)
    {
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);
    }

    if (SPI_processed > 0)
    {
        buf = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        if (NULL != mode && NULL != buf)
        {
            memcpy(mode, buf, strlen(buf));
        }
    }
    pfree(querybuf.data);

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    MemoryContextSwitchTo(originalContext);
    pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * query timezone from cron.lt_job_ext where == jobid
 */
void
queryZoneFromCronExt(int64 jobid, int *tmzone)
{
    StringInfoData querybuf;
    MemoryContext originalContext = CurrentMemoryContext;
    char *buf = NULL;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    if (!PgCronHasBeenLoaded() || RecoveryInProgress() || !JobLtExtTableExists())
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
        MemoryContextSwitchTo(originalContext);
        return;
    }

    initStringInfo(&querybuf);

    /* Open SPI context. */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(ERROR, "SPI_connect failed");
    }

    appendStringInfo(&querybuf, "select timezone from %s where jobid = %lld",
             quote_qualified_identifier(CRON_SCHEMA_NAME, LT_JOB_EXT), (long long int)jobid);

    pgstat_report_activity(STATE_RUNNING, querybuf.data);

    if (SPI_exec(querybuf.data, 0) != SPI_OK_SELECT)
    {
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);
    }

    if (SPI_processed > 0)
    {
        buf = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        if (NULL != tmzone && NULL != buf)
        {
            (*tmzone) = atoi(buf);
        }
    }
    pfree(querybuf.data);

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    MemoryContextSwitchTo(originalContext);
    pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * update active into cron.job where == jobid
 */
void
updateCronActive(int64 jobid, char *active)
{
    StringInfoData updatebuf;
    MemoryContext originalContext = CurrentMemoryContext;

    if (NULL == active)
    {
        elog(ERROR, "active is null");
    }

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    if (!PgCronHasBeenLoaded() || RecoveryInProgress())
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
        MemoryContextSwitchTo(originalContext);
        return;
    }

    initStringInfo(&updatebuf);

    /* Open SPI context. */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(ERROR, "SPI_connect failed");
    }

    appendStringInfo(&updatebuf, "update %s set active = '%s' where jobid = %lld",
                     quote_qualified_identifier(CRON_SCHEMA_NAME, JOBS_TABLE_NAME), active, (long long int)jobid);

    pgstat_report_activity(STATE_RUNNING, updatebuf.data);

    if (SPI_exec(updatebuf.data, 0) != SPI_OK_UPDATE)
    {
        elog(ERROR, "SPI_exec failed: %s", updatebuf.data);
    }

    pfree(updatebuf.data);

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    MemoryContextSwitchTo(originalContext);
    pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Keep 100000 rows of table cron.job_run_details up-to-date data
 */
void
keepDataFromCronRun()
{
    StringInfoData querybuf;
    StringInfoData deletebuf;
    MemoryContext originalContext = CurrentMemoryContext;
    int row_num = 0;
    char *buf = NULL;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    if (!PgCronHasBeenLoaded() || RecoveryInProgress() || !JobRunDetailsTableExists())
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
        MemoryContextSwitchTo(originalContext);
        return;
    }

    initStringInfo(&querybuf);

    /* Open SPI context. */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(ERROR, "SPI_connect failed");
    }

    appendStringInfo(&querybuf, "select count(*) from %s",
                     quote_qualified_identifier(CRON_SCHEMA_NAME, JOB_RUN_DETAILS_TABLE_NAME));

    pgstat_report_activity(STATE_RUNNING, querybuf.data);

    if (SPI_exec(querybuf.data, 0) != SPI_OK_SELECT)
    {
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);
    }

    if (SPI_processed > 0)
    {
        buf = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        if (NULL != buf)
        {
            row_num = atoi(buf);
        }
    }
    pfree(querybuf.data);

    if (100000 < row_num)
    {
        initStringInfo(&deletebuf);

        appendStringInfo(&deletebuf, "delete from %s where runid in (select runid from %s order by runid desc limit (select count(runid) from %s) offset 100000)",
             quote_qualified_identifier(CRON_SCHEMA_NAME, JOB_RUN_DETAILS_TABLE_NAME), quote_qualified_identifier(CRON_SCHEMA_NAME, JOB_RUN_DETAILS_TABLE_NAME),
                         quote_qualified_identifier(CRON_SCHEMA_NAME, JOB_RUN_DETAILS_TABLE_NAME));

        pgstat_report_activity(STATE_RUNNING, deletebuf.data);

        if (SPI_exec(deletebuf.data, 0) != SPI_OK_DELETE)
        {
            elog(ERROR, "SPI_exec failed: %s", deletebuf.data);
        }

        pfree(deletebuf.data);
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    MemoryContextSwitchTo(originalContext);
    pgstat_report_activity(STATE_IDLE, NULL);
}
