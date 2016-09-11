/*-------------------------------------------------------------------------
 *
 * src/pg_cron.c
 *
 * Implementation of the pg_cron task scheduler.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

/* these are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */

#define MAIN_PROGRAM
#include "cron.h"

#include "pg_cron.h"
#include "cron_job.h"

#include "poll.h"
#include "sys/time.h"
#include "sys/poll.h"
#include "time.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "tcop/utility.h"


#define CRON_SCHEMA_NAME "cron"
#define JOBS_TABLE_NAME "job"
#define JOB_ID_INDEX_NAME "job_pkey"
#define JOB_ID_SEQUENCE_NAME "cron.jobid_seq"


PG_MODULE_MAGIC;


void _PG_init(void);
void _PG_fini(void);
static void pg_cron_sigterm(SIGNAL_ARGS);
static void pg_cron_sighup(SIGNAL_ARGS);
static void PgCronWorkerMain(Datum arg);

static void InvalidateJobCacheCallback(Datum argument, Oid relationId);
static void InvalidateJobCache(void);
static Oid CronJobRelationId(void);

static void ReloadCronJobs(void);
static List * LoadCronJobList(void);
static CronJob * TupleToCronJob(TupleDesc tupleDescriptor, HeapTuple heapTuple);
static bool PgCronHasBeenLoaded(void);

static void StartAllPendingRuns(List *taskList, TimestampTz currentTime);
static void StartPendingRuns(CronTask *task, ClockProgress clockProgress,
							 TimestampTz lastMinute, TimestampTz currentTime);
static int MinutesPassed(TimestampTz startTime, TimestampTz stopTime);
static TimestampTz TimestampMinuteStart(TimestampTz time);
static TimestampTz TimestampMinuteEnd(TimestampTz time);
static bool ShouldRunTask(entry *schedule, TimestampTz currentMinute,
						  bool doWild, bool doNonWild);

static List * CurrentTaskList(void);
static void WaitForCronTasks(List *taskList);
static void PollForTasks(List *taskList);
static void ManageCronTasks(List *taskList, TimestampTz currentTime);
static void ManageCronTask(CronTask *task, TimestampTz currentTime);

static HTAB * CreateCronJobHash(void);
static HTAB * CreateCronTaskHash(void);
static CronJob * GetCronJob(int64 jobId);
static CronTask * GetCronTask(int64 jobId);
static void InitializeCronTask(CronTask *task, int64 jobId);


/* flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;

static MemoryContext CronJobContext = NULL;
static MemoryContext CronTaskContext = NULL;
static HTAB *CronJobHash = NULL;
static HTAB *CronTaskHash = NULL;
static bool CronJobCacheValid = false;
static Oid CachedCronJobRelationId = InvalidOid;
static bool RebootJobsScheduled = false;
static int64 RunCount = 0;

static char *CronTableDatabaseName = "postgres";
static int CronTaskStartTimeout = 10000; /* maximum connection time */
static const int MaxWait = 1000; /* maximum time in ms that poll() can block */


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(cron_schedule);
PG_FUNCTION_INFO_V1(cron_unschedule);
PG_FUNCTION_INFO_V1(cron_job_cache_invalidate);


/*
 * _PG_init gets called when the extension is loaded.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("pg_cron can only be loaded via shared_preload_libraries"),
						errhint("Add pg_cron to shared_preload_libraries configuration "
								"variable in postgresql.conf in master and workers.")));
	}

	DefineCustomStringVariable(
		"cron.database_name",
		gettext_noop("Database in which pg_cron metadata is kept."),
		NULL,
		&CronTableDatabaseName,
		"postgres",
		PGC_POSTMASTER,
		GUC_SUPERUSER_ONLY,
		NULL, NULL, NULL);

	/* watch for invalidation events */
	CacheRegisterRelcacheCallback(InvalidateJobCacheCallback, (Datum) 0);

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = PgCronWorkerMain;
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;
	sprintf(worker.bgw_library_name, "pg_cron");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_cron_scheduler");

	RegisterBackgroundWorker(&worker);
}


/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_cron_sigterm(SIGNAL_ARGS)
{
	got_sigterm = true;
}


/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reload the cron jobs.
 */
static void
pg_cron_sighup(SIGNAL_ARGS)
{
	CronJobCacheValid = false;
}


/*
 * PgCronWorkerMain is the main entry-point for the background worker
 * that performs tasks.
 */
static void
PgCronWorkerMain(Datum arg)
{
	MemoryContext CronLoopContext = NULL;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_cron_sighup);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, pg_cron_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(CronTableDatabaseName, NULL);

	CronJobContext = AllocSetContextCreate(CurrentMemoryContext,
										   "pg_cron job context",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

	CronTaskContext = AllocSetContextCreate(CurrentMemoryContext,
											"pg_cron task context",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);

	CronLoopContext = AllocSetContextCreate(CurrentMemoryContext,
											"pg_cron loop context",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);

	CronJobHash = CreateCronJobHash();
	CronTaskHash = CreateCronTaskHash();

	elog(LOG, "pg_cron scheduler started");

	MemoryContextSwitchTo(CronLoopContext);

	while (!got_sigterm)
	{
		List *taskList = NIL;
		TimestampTz currentTime = 0;

		AcceptInvalidationMessages();

		if (!CronJobCacheValid)
		{
			ReloadCronJobs();
		}

		taskList = CurrentTaskList();
		currentTime = GetCurrentTimestamp();

		StartAllPendingRuns(taskList, currentTime);

		WaitForCronTasks(taskList);
		ManageCronTasks(taskList, currentTime);

		MemoryContextReset(CronLoopContext);
	}

	elog(LOG, "pg_cron scheduler shutting down");

	proc_exit(0);
}


/*
 * cluster_schedule schedules a cron job.
 */
Datum
cron_schedule(PG_FUNCTION_ARGS)
{
	text *scheduleText = PG_GETARG_TEXT_P(0);
	text *commandText = PG_GETARG_TEXT_P(1);

	char *schedule = text_to_cstring(scheduleText);
	char *command = text_to_cstring(commandText);
	entry *parsedSchedule = NULL;

	int64 jobId = 0;
	Datum jobIdDatum = 0;
	Datum jobIdSequenceName = 0;

	Oid cronSchemaId = InvalidOid;
	Oid cronJobsRelationId = InvalidOid;

	Relation cronJobsTable = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_cron_job];
	bool isNulls[Natts_cron_job];

	Oid userId = GetUserId();
	char *userName = GetUserNameFromId(userId, false);

	parsedSchedule = parse_cron_entry(schedule);
	if (parsedSchedule == NULL)
	{
		free_entry(parsedSchedule);

		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid schedule: %s", schedule)));
	}

	free_entry(parsedSchedule);

	/* form new job tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	jobIdSequenceName = CStringGetTextDatum(JOB_ID_SEQUENCE_NAME);
	jobIdDatum = DirectFunctionCall1(nextval, jobIdSequenceName);
	jobId = DatumGetInt64(jobIdDatum);

	values[Anum_cron_job_jobid - 1] = jobIdDatum;
	values[Anum_cron_job_schedule - 1] = CStringGetTextDatum(schedule);
	values[Anum_cron_job_command - 1] = CStringGetTextDatum(command);
	values[Anum_cron_job_nodename - 1] = CStringGetTextDatum("localhost");
	values[Anum_cron_job_nodeport - 1] = Int32GetDatum(PostPortNumber);
	values[Anum_cron_job_database - 1] = CStringGetTextDatum(CronTableDatabaseName);
	values[Anum_cron_job_username - 1] = CStringGetTextDatum(userName);

	cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
	cronJobsRelationId = get_relname_relid(JOBS_TABLE_NAME, cronSchemaId);

	/* open jobs relation and insert new tuple */
	cronJobsTable = heap_open(cronJobsRelationId, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(cronJobsTable);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(cronJobsTable, heapTuple);
	CatalogUpdateIndexes(cronJobsTable, heapTuple);
	CommandCounterIncrement();

	/* close relation and invalidate previous cache entry */
	heap_close(cronJobsTable, RowExclusiveLock);

	InvalidateJobCache();

	PG_RETURN_INT64(jobId);
}


/*
 * cluster_unschedule removes a cron job.
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
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	bool isNull = false;
	Oid userId = InvalidOid;
	char *userName = NULL;
	Datum ownerNameDatum = 0;
	char *ownerName = NULL;

	cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
	cronJobIndexId = get_relname_relid(JOB_ID_INDEX_NAME, cronSchemaId);

	cronJobsTable = heap_open(CronJobRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_cron_job_jobid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

	scanDescriptor = systable_beginscan(cronJobsTable,
										cronJobIndexId, indexOK,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(cronJobsTable);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for job "
							   UINT64_FORMAT, jobId)));
	}

	/* check if the current user owns the row */
	userId = GetUserId();
	userName = GetUserNameFromId(userId, false);

	ownerNameDatum = heap_getattr(heapTuple, Anum_cron_job_username,
								  tupleDescriptor, &isNull);
	ownerName = TextDatumGetCString(ownerNameDatum);
	if (pg_strcasecmp(userName, ownerName) != 0)
	{
		/* otherwise, allow if the user has DELETE permission */
		AclResult aclResult = pg_class_aclcheck(CronJobRelationId(), GetUserId(),
												ACL_DELETE);
		if (aclResult != ACLCHECK_OK)
		{
			aclcheck_error(aclResult, ACL_KIND_CLASS,
						   get_rel_name(CronJobRelationId()));
		}
	}

	simple_heap_delete(cronJobsTable, &heapTuple->t_self);
	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(cronJobsTable, RowExclusiveLock);

	InvalidateJobCache();

	PG_RETURN_BOOL(true);
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
 * ReloadCronJobs reloads the cron jobs from the cron.job table.
 * If a job that has an active task has been removed, the task
 * is marked as inactive by this function.
 */
static void
ReloadCronJobs(void)
{
	List *jobList = NIL;
	ListCell *jobCell = NULL;
	CronTask *task = NULL;
	HASH_SEQ_STATUS status;

	/* destroy old job hash */
	MemoryContextResetAndDeleteChildren(CronJobContext);

	CronJobHash = CreateCronJobHash();

	hash_seq_init(&status, CronTaskHash);

	/* mark all tasks as inactive */
	while ((task = hash_seq_search(&status)) != NULL)
	{
		task->isActive = false;
	}

	jobList = LoadCronJobList();

	/* mark tasks that still have a job as active */
	foreach(jobCell, jobList)
	{
		CronJob *job = (CronJob *) lfirst(jobCell);

		CronTask *task = GetCronTask(job->jobId);
		task->isActive = true;
	}

	CronJobCacheValid = true;
}


/*
 * LoadCronJobList loads the current list of jobs from the
 * cron.job table and adds each job to the CronJobHash.
 */
static List *
LoadCronJobList(void)
{
	List *jobList = NIL;

	Relation cronJobTable = NULL;

	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;

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
		pgstat_report_activity(STATE_IDLE, NULL);

		return NIL;
	}

	cronJobTable = heap_open(CronJobRelationId(), AccessShareLock);

	scanDescriptor = systable_beginscan(cronJobTable,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(cronJobTable);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		MemoryContext oldContext = NULL;
		CronJob *job = NULL;

		oldContext = MemoryContextSwitchTo(CronJobContext);

		job = TupleToCronJob(tupleDescriptor, heapTuple);
		jobList = lappend(jobList, job);

		MemoryContextSwitchTo(oldContext);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(cronJobTable, AccessShareLock);

	PopActiveSnapshot();
	CommitTransactionCommand();
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

	Assert(!HeapTupleHasNulls(heapTuple));

	jobKey = DatumGetUInt32(jobId);
	job = hash_search(CronJobHash, &jobKey, HASH_ENTER, &isPresent);

	job->jobId = DatumGetUInt32(jobId);
	job->scheduleText = TextDatumGetCString(schedule);
	job->command = TextDatumGetCString(command);
	job->nodeName = TextDatumGetCString(nodeName);
	job->nodePort = DatumGetUInt32(nodePort);
	job->userName = TextDatumGetCString(userName);
	job->database = TextDatumGetCString(database);

	parsedSchedule = parse_cron_entry(job->scheduleText);
	if (parsedSchedule != NULL)
	{
		/* copy the schedule and free the allocated memory immediately */

		job->schedule = *parsedSchedule;
		free_entry(parsedSchedule);
	}
	else
	{
		elog(LOG, "invalid pg_cron schedule for job %ld: %s", jobId, job->scheduleText);

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

	Oid extensionOid = get_extension_oid("pg_cron", true);
	if (extensionOid != InvalidOid)
	{
		extensionPresent = true;
	}

	if (extensionPresent)
	{
		/* check if PgCron extension objects are still being created */
		if (creating_extension && CurrentExtensionObject == extensionOid)
		{
			extensionScriptExecuted = false;
		}
	}

	extensionLoaded = extensionPresent && extensionScriptExecuted;

	return extensionLoaded;
}


/*
 * StartPendingRuns goes through the list of tasks and kicks of
 * runs for tasks that should start, taking clock changes into
 * into consideration.
 */
static void
StartAllPendingRuns(List *taskList, TimestampTz currentTime)
{
	static TimestampTz lastMinute = 0;

	int minutesPassed = 0;
	ListCell *taskCell = NULL;
	ClockProgress clockProgress;

	if (!RebootJobsScheduled)
	{
		/* find jobs with @reboot as a schedule */
		foreach(taskCell, taskList)
		{
			CronTask *task = (CronTask *) lfirst(taskCell);
			CronJob *cronJob = GetCronJob(task->jobId);
			entry *schedule = &cronJob->schedule;

			if (schedule->flags & WHEN_REBOOT)
			{
				task->pendingRunCount += 1;
			}
		}

		RebootJobsScheduled = true;
	}

	if (lastMinute == 0)
	{
		lastMinute = TimestampMinuteStart(currentTime);
	}

	minutesPassed = MinutesPassed(lastMinute, currentTime);
	if (minutesPassed == 0)
	{
		/* wait for new minute */
		return;
	}

	/* use Vixie cron logic for clock jumps */
	if (minutesPassed > (3*MINUTE_COUNT))
	{
		/* clock jumped forward by more than 3 hours */
		clockProgress = CLOCK_CHANGE;
	}
	else if (minutesPassed > 5)
	{
		/* clock went forward by more than 5 minutes (DST?) */
		clockProgress = CLOCK_JUMP_FORWARD;
	}
	else if (minutesPassed > 0)
	{
		/* clock went forward by 1-5 minutes */
		clockProgress = CLOCK_PROGRESSED;
	}
	else if (minutesPassed > -(3*MINUTE_COUNT))
	{
		/* clock jumped backwards by less than 3 hours (DST?) */
		clockProgress = CLOCK_JUMP_BACKWARD;
	}
	else
	{
		/* clock jumped backwards 3 hours or more */
		clockProgress = CLOCK_CHANGE;
	}

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);

		StartPendingRuns(task, clockProgress, lastMinute, currentTime);
	}

	/*
	 * If the clock jump backwards then we avoid repeating the fixed-time
	 * tasks by preserving the last minute from before the clock jump,
	 * until the clock has caught up (clockProgress will be
	 * CLOCK_JUMP_BACKWARD until then).
	 */
	if (clockProgress != CLOCK_JUMP_BACKWARD)
	{
		lastMinute = TimestampMinuteStart(currentTime);
	}
}


/*
 * StartPendingRuns kicks off pending runs for a task if it
 * should start, taking clock changes into consideration.
 */
static void
StartPendingRuns(CronTask *task, ClockProgress clockProgress,
				 TimestampTz lastMinute, TimestampTz currentTime)
{
	CronJob *cronJob = GetCronJob(task->jobId);
	entry *schedule = &cronJob->schedule;
	TimestampTz virtualTime = lastMinute;
	TimestampTz currentMinute = TimestampMinuteStart(currentTime);


	switch (clockProgress)
	{
		case CLOCK_PROGRESSED:
		{
			/*
			 * case 1: minutesPassed is a small positive number
			 * run jobs for each virtual minute until caught up.
			 */

			do
			{
				virtualTime = TimestampTzPlusMilliseconds(virtualTime,
														  60*1000);

				if (ShouldRunTask(schedule, virtualTime, true, true))
				{
					task->pendingRunCount += 1;
				}
			}
			while (virtualTime < currentMinute);

			break;
		}

		case CLOCK_JUMP_FORWARD:
		{
			/*
			 * case 2: minutesPassed is a medium-sized positive number,
			 * for example because we went to DST run wildcard
			 * jobs once, then run any fixed-time jobs that would
			 * otherwise be skipped if we use up our minute
			 * (possible, if there are a lot of jobs to run) go
			 * around the loop again so that wildcard jobs have
			 * a chance to run, and we do our housekeeping
			 */

			/* run fixed-time jobs for each minute missed */
			do
			{
				virtualTime = TimestampTzPlusMilliseconds(virtualTime,
														  60*1000);

				if (ShouldRunTask(schedule, virtualTime, false, true))
				{
					task->pendingRunCount += 1;
				}

			} while (virtualTime < currentMinute);

			/* run wildcard jobs for current minute */
			if (ShouldRunTask(schedule, currentMinute, true, false))
			{
				task->pendingRunCount += 1;
			}

			break;
		}

		case CLOCK_JUMP_BACKWARD:
		{
			/*
			 * case 3: timeDiff is a small or medium-sized
			 * negative num, eg. because of DST ending just run
			 * the wildcard jobs. The fixed-time jobs probably
			 * have already run, and should not be repeated
			 * virtual time does not change until we are caught up
			 */

			if (ShouldRunTask(schedule, currentMinute, true, false))
			{
				task->pendingRunCount += 1;
			}

			break;
		}

		default:
		{
			/*
			 * other: time has changed a *lot*, skip over any
			 * intermediate fixed-time jobs and go back to
			 * normal operation.
			 */
			if (ShouldRunTask(schedule, currentMinute, true, true))
			{
				task->pendingRunCount += 1;
			}
		}
	}
}


/*
 * MinutesPassed returns the number of minutes between startTime and
 * stopTime rounded down to the closest integer.
 */
static int
MinutesPassed(TimestampTz startTime, TimestampTz stopTime)
{
	int microsPassed = 0;
	long secondsPassed = 0;
	int minutesPassed = 0;

	TimestampDifference(startTime, stopTime,
						&secondsPassed, &microsPassed);

	minutesPassed = secondsPassed / 60;

	return minutesPassed;
}


/*
 * TimestampMinuteEnd returns the timestamp at the start of the
 * current minute for the given time.
 */
static TimestampTz
TimestampMinuteStart(TimestampTz time)
{
	TimestampTz result = 0;

#ifdef HAVE_INT64_TIMESTAMP
	result = time - time % 60000000;
#else
	result = (long) time - (long) time % 60;
#endif

	return result;
}


/*
 * TimestampMinuteEnd returns the timestamp at the start of the
 * next minute from the given time.
 */
static TimestampTz
TimestampMinuteEnd(TimestampTz time)
{
	TimestampTz result = TimestampMinuteStart(time);

#ifdef HAVE_INT64_TIMESTAMP
	result += 60000000;
#else
	result += 60;
#endif

	return result;
}


/*
 * ShouldRunTask returns whether a job should run in the current
 * minute according to its schedule.
 */
static bool
ShouldRunTask(entry *schedule, TimestampTz currentTime, bool doWild,
			  bool doNonWild)
{
	time_t currentTime_t = timestamptz_to_time_t(currentTime);
	struct tm *tm = gmtime(&currentTime_t);

	int minute = tm->tm_min -FIRST_MINUTE;
	int hour = tm->tm_hour -FIRST_HOUR;
	int dayOfMonth = tm->tm_mday -FIRST_DOM;
	int month = tm->tm_mon +1 -FIRST_MONTH;
	int dayOfWeek = tm->tm_wday -FIRST_DOW;

	if (bit_test(schedule->minute, minute) &&
	    bit_test(schedule->hour, hour) &&
	    bit_test(schedule->month, month) &&
	    ( ((schedule->flags & DOM_STAR) || (schedule->flags & DOW_STAR))
	      ? (bit_test(schedule->dow,dayOfWeek) && bit_test(schedule->dom,dayOfMonth))
	      : (bit_test(schedule->dow,dayOfWeek) || bit_test(schedule->dom,dayOfMonth)))) {
		if ((doNonWild && !(schedule->flags & (MIN_STAR|HR_STAR)))
		    || (doWild && (schedule->flags & (MIN_STAR|HR_STAR))))
		{
			return true;
		}
	}

	return false;
}


/*
 * CurrentTaskList extracts the current list of tasks from the
 * cron task hash.
 */
static List *
CurrentTaskList(void)
{
	List *taskList = NIL;
	CronTask *task = NULL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, CronTaskHash);

	while ((task = hash_seq_search(&status)) != NULL)
	{
		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * WaitForCronTasks blocks waiting for any active task for at most
 * 1 second.
 */
static void
WaitForCronTasks(List *taskList)
{
	int taskCount = list_length(taskList);

	if (taskCount > 0)
	{
		PollForTasks(taskList);
	}
	else
	{
		/* wait for new jobs */
		pg_usleep(MaxWait*1000L);
	}
}


/*
 * PollForTasks calls poll() for the sockets of all tasks. It checks for
 * read or write events based on the pollingStatus of the task.
 */
static void
PollForTasks(List *taskList)
{
	TimestampTz currentTime = 0;
	TimestampTz nextEventTime = 0;
	int pollTimeout = 0;
	long waitSeconds = 0;
	int waitMicros = 0;
	struct pollfd *pollFDs = NULL;
	int pollResult = 0;

	int taskIndex = 0;
	int taskCount = list_length(taskList);
	ListCell *taskCell = NULL;

	pollFDs = (struct pollfd *) palloc0(taskCount * sizeof(struct pollfd));

	currentTime = GetCurrentTimestamp();

	/*
	 * At the latest, wake up when the next minute starts.
	 */
	nextEventTime = TimestampMinuteEnd(currentTime);

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);
		PostgresPollingStatusType pollingStatus = task->pollingStatus;
		struct pollfd *pollFileDescriptor = &pollFDs[taskIndex];

		if ((task->state == CRON_TASK_WAITING && task->pendingRunCount > 0) ||
			task->state == CRON_TASK_ERROR || task->state == CRON_TASK_DONE)
		{
			/* there is work to be done, don't wait */
			pfree(pollFDs);
			return;
		}

		if (task->state == CRON_TASK_CONNECTING ||
			task->state == CRON_TASK_SENDING)
		{
			/*
			 * We need to wake up when a timeout expires.
			 * Take the minimum of nextEventTime and task->startDeadline.
			 */
			if (TimestampDifferenceExceeds(task->startDeadline, nextEventTime, 0))
			{
				nextEventTime = task->startDeadline;
			}
		}

		if (task->state == CRON_TASK_CONNECTING ||
			task->state == CRON_TASK_SENDING ||
			task->state == CRON_TASK_RUNNING)
		{
			PGconn *connection = task->connection;
			int pollEventMask = 0;

			/*
			 * Set the appropriate mask for poll, based on the current polling
			 * status of the task, controlled by ManageCronTask.
			 */

			if (pollingStatus == PGRES_POLLING_READING)
			{
				pollEventMask = POLLERR | POLLIN;
			}
			else if (pollingStatus == PGRES_POLLING_WRITING)
			{
				pollEventMask = POLLERR | POLLOUT;
			}

			pollFileDescriptor->fd = PQsocket(connection);
			pollFileDescriptor->events = pollEventMask;
		}
		else
		{
			/*
			 * Task is not running.
			 */

			pollFileDescriptor->fd = -1;
			pollFileDescriptor->events = 0;
		}

		pollFileDescriptor->revents = 0;

		taskIndex++;
	}

	/*
	 * Find the first time-based event, which is either the start of a new
	 * minute or a timeout.
	 */
	TimestampDifference(currentTime, nextEventTime, &waitSeconds, &waitMicros);

	pollTimeout = waitSeconds * 1000 + waitMicros / 1000;
	if (pollTimeout <= 0)
	{
		pfree(pollFDs);
		return;
	}
	else if (pollTimeout > MaxWait)
	{
		/*
		 * We never wait more than 1 second, this gives us a chance to react
		 * to external events like a TERM signal and job changes.
		 */

		pollTimeout = MaxWait;
	}

	pollResult = poll(pollFDs, taskCount, pollTimeout);
	if (pollResult < 0)
	{
		/*
		 * This typically happens in case of a signal, though we should
		 * probably check errno in case something bad happened.
		 */

		pfree(pollFDs);
		return;
	}

	taskIndex = 0;

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);
		struct pollfd *pollFileDescriptor = &pollFDs[taskIndex];

		task->isSocketReady = pollFileDescriptor->revents &
							  pollFileDescriptor->events;

		taskIndex++;
	}

	pfree(pollFDs);
}


/*
 * ManageCronTasks proceeds the state machines of the given list of tasks.
 */
static void
ManageCronTasks(List *taskList, TimestampTz currentTime)
{
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);

		ManageCronTask(task, currentTime);
	}
}


/*
 * ManageCronTask implements the cron task state machine.
 */
static void
ManageCronTask(CronTask *task, TimestampTz currentTime)
{
	CronTaskState checkState = task->state;
	int64 jobId = task->jobId;
	CronJob *cronJob = GetCronJob(jobId);
	PGconn *connection = task->connection;
	ConnStatusType connectionStatus = CONNECTION_BAD;

	switch (checkState)
	{
		case CRON_TASK_WAITING:
		{
			/* check if job has been removed */
			if (!task->isActive)
			{
				/* remove task as well */
				bool isPresent = false;
				hash_search(CronTaskHash, &jobId, HASH_REMOVE, &isPresent);
				break;
			}

			/* check whether runs are pending */
			if (task->pendingRunCount == 0)
			{
				break;
			}

			task->runId = RunCount++;
			task->pendingRunCount -= 1;
			task->state = CRON_TASK_START;
		}

		case CRON_TASK_START:
		{
			const char *clientEncoding = GetDatabaseEncodingName();
			char nodePortString[12];
			TimestampTz startDeadline = 0;

			const char *keywordArray[] = {
				"host",
				"port",
				"fallback_application_name",
				"client_encoding",
				"dbname",
				"user",
				NULL
			};
			const char *valueArray[] = {
				cronJob->nodeName,
				nodePortString,
				"pg_cron",
				clientEncoding,
				cronJob->database,
				cronJob->userName,
				NULL
			};
			sprintf(nodePortString, "%d", cronJob->nodePort);

			Assert(sizeof(keywordArray) == sizeof(valueArray));

			connection = PQconnectStartParams(keywordArray, valueArray, false);
			PQsetnonblocking(connection, 1);

			connectionStatus = PQstatus(connection);
			if (connectionStatus == CONNECTION_BAD)
			{
				task->errorMessage = "connection failed";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			startDeadline = TimestampTzPlusMilliseconds(currentTime,
														CronTaskStartTimeout);

			task->startDeadline = startDeadline;
			task->connection = connection;
			task->pollingStatus = PGRES_POLLING_WRITING;
			task->state = CRON_TASK_CONNECTING;

			break;
		}

		case CRON_TASK_CONNECTING:
		{
			PostgresPollingStatusType pollingStatus = 0;

			/* check if job has been removed */
			if (!task->isActive)
			{
				task->errorMessage = "job cancelled";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if timeout has been reached */
			if (TimestampDifferenceExceeds(task->startDeadline, currentTime, 0))
			{
				task->errorMessage = "connection timeout";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if connection is still alive */
			connectionStatus = PQstatus(connection);
			if (connectionStatus == CONNECTION_BAD)
			{
				task->errorMessage = "connection failed";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if socket is ready to send */
			if (!task->isSocketReady)
			{
				break;
			}

			/* check whether a connection has been established */
			pollingStatus = PQconnectPoll(connection);
			if (pollingStatus == PGRES_POLLING_OK)
			{
				/* wait for socket to be ready to send a query */
				task->pollingStatus = PGRES_POLLING_WRITING;

				task->state = CRON_TASK_SENDING;
			}
			else if (pollingStatus == PGRES_POLLING_FAILED)
			{
				task->errorMessage = "connection failed";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
			}
			else
			{
				/*
				 * Connection is still being established.
				 *
				 * On the next WaitForTasks round, we wait for reading or writing
				 * based on the status returned by PQconnectPoll, see:
				 * https://www.postgresql.org/docs/9.5/static/libpq-connect.html
				 */
				task->pollingStatus = pollingStatus;
			}

			break;
		}

		case CRON_TASK_SENDING:
		{
			char *command = cronJob->command;
			int sendResult = 0;

			/* check if job has been removed */
			if (!task->isActive)
			{
				task->errorMessage = "job cancelled";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if timeout has been reached */
			if (TimestampDifferenceExceeds(task->startDeadline, currentTime, 0))
			{
				task->errorMessage = "connection timeout";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if socket is ready to send */
			if (!task->isSocketReady)
			{
				break;
			}

			/* check if connection is still alive */
			connectionStatus = PQstatus(connection);
			if (connectionStatus == CONNECTION_BAD)
			{
				task->errorMessage = "connection lost";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			sendResult = PQsendQuery(connection, command);
			if (sendResult == 1)
			{
				/* wait for socket to be ready to receive results */
				task->pollingStatus = PGRES_POLLING_READING;

				/* command is underway, stop using timeout */
				task->startDeadline = 0;
				task->state = CRON_TASK_RUNNING;
			}
			else
			{
				/* not yet ready to send */
			}

			break;
		}

		case CRON_TASK_RUNNING:
		{
			int connectionBusy = 0;
			PGresult *result = NULL;

			/* check if job has been removed */
			if (!task->isActive)
			{
				task->errorMessage = "job cancelled";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if connection is still alive */
			connectionStatus = PQstatus(connection);
			if (connectionStatus == CONNECTION_BAD)
			{
				task->errorMessage = "connection lost";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if socket is ready to send */
			if (!task->isSocketReady)
			{
				break;
			}

			PQconsumeInput(connection);

			connectionBusy = PQisBusy(connection);
			if (connectionBusy)
			{
				/* still waiting for results */
				break;
			}

			while ((result = PQgetResult(connection)) != NULL)
			{
				ExecStatusType executionStatus = PQresultStatus(result);

				switch (executionStatus)
				{
					case PGRES_TUPLES_OK:
					{
						break;
					}

					case PGRES_COMMAND_OK:
					{
						break;
					}

					case PGRES_BAD_RESPONSE:
					case PGRES_FATAL_ERROR:
					{
						task->errorMessage = PQresultErrorMessage(result);
						task->pollingStatus = 0;
						task->state = CRON_TASK_ERROR;

						PQclear(result);

						return;
					}

					case PGRES_COPY_IN:
					case PGRES_COPY_OUT:
					case PGRES_COPY_BOTH:
					{
						/* cannot handle COPY input/output */
						task->errorMessage = "COPY not supported";
						task->pollingStatus = 0;
						task->state = CRON_TASK_ERROR;

						PQclear(result);

						return;
					}

					case PGRES_EMPTY_QUERY:
					case PGRES_SINGLE_TUPLE:
					case PGRES_NONFATAL_ERROR:
					default:
					{
						break;
					}

				}

				PQclear(result);
			}

			PQfinish(connection);

			task->connection = NULL;
			task->pollingStatus = 0;
			task->isSocketReady = false;
			task->state = CRON_TASK_DONE;

			break;
		}

		case CRON_TASK_ERROR:
		{
			if (connection != NULL)
			{
				PQfinish(connection);
				task->connection = NULL;
			}

			if (!task->isActive)
			{
				bool isPresent = false;
				hash_search(CronTaskHash, &jobId, HASH_REMOVE, &isPresent);
			}

			if (task->errorMessage != NULL)
			{
				elog(LOG, "pg_cron job %ld: %s", jobId, task->errorMessage);
			}

			task->startDeadline = 0;
			task->isSocketReady = false;
			task->state = CRON_TASK_DONE;

			/* fall through to CRON_TASK_DONE */
		}

		case CRON_TASK_DONE:
		default:
		{
			InitializeCronTask(task, jobId);
		}

	}
}


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


static HTAB *
CreateCronTaskHash(void)
{
	HTAB *taskHash = NULL;
	HASHCTL info;
	int hashFlags = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(CronTask);
	info.hash = tag_hash;
	info.hcxt = CronTaskContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	taskHash = hash_create("pg_cron tasks", 32, &info, hashFlags);

	return taskHash;
}


/*
 * GetCronTask gets the current task with the given job ID.
 */
static CronTask *
GetCronTask(int64 jobId)
{
	CronTask *task = NULL;
	int64 hashKey = jobId;
	bool isPresent = false;

	task = hash_search(CronTaskHash, &hashKey, HASH_ENTER, &isPresent);
	if (!isPresent)
	{
		InitializeCronTask(task, jobId);
	}

	return task;
}


/*
 * InitializeCronTask intializes a CronTask struct.
 */
static void
InitializeCronTask(CronTask *task, int64 jobId)
{
	task->runId = 0;
	task->jobId = jobId;
	task->state = CRON_TASK_WAITING;
	task->pendingRunCount = 0;
	task->connection = NULL;
	task->pollingStatus = 0;
	task->startDeadline = 0;
	task->isSocketReady = false;
	task->isActive = true;
	task->errorMessage = NULL;
}


/*
 * GetCronJob gets the cron job with the given id.
 */
static CronJob *
GetCronJob(int64 jobId)
{
	CronJob *job = NULL;
	int64 hashKey = jobId;
	bool isPresent = false;

	job = hash_search(CronJobHash, &hashKey, HASH_FIND, &isPresent);

	return job;
}
