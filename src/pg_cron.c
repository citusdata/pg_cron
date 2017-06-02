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
#include "task_states.h"
#include "job_metadata.h"

#include "poll.h"
#include "sys/time.h"
#include "sys/poll.h"
#include "time.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_extension.h"
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


PG_MODULE_MAGIC;


/* ways in which the clock can change between main loop iterations */
typedef enum
{
	CLOCK_JUMP_BACKWARD = 0,
	CLOCK_PROGRESSED = 1,
	CLOCK_JUMP_FORWARD = 2,
	CLOCK_CHANGE = 3
} ClockProgress;


/* forward declarations */
void _PG_init(void);
void _PG_fini(void);
static void pg_cron_sigterm(SIGNAL_ARGS);
static void pg_cron_sighup(SIGNAL_ARGS);
static void PgCronWorkerMain(Datum arg);

static void StartAllPendingRuns(List *taskList, TimestampTz currentTime);
static void StartPendingRuns(CronTask *task, ClockProgress clockProgress,
							 TimestampTz lastMinute, TimestampTz currentTime);
static int MinutesPassed(TimestampTz startTime, TimestampTz stopTime);
static TimestampTz TimestampMinuteStart(TimestampTz time);
static TimestampTz TimestampMinuteEnd(TimestampTz time);
static bool ShouldRunTask(entry *schedule, TimestampTz currentMinute,
						  bool doWild, bool doNonWild);

static void WaitForCronTasks(List *taskList);
static void PollForTasks(List *taskList);
static void ManageCronTasks(List *taskList, TimestampTz currentTime);
static void ManageCronTask(CronTask *task, TimestampTz currentTime);


/* global settings */
char *CronTableDatabaseName = "postgres";
static bool CronLogStatement = true;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;

/* global variables */
static int64 RunCount = 0; /* counter for assigning unique run IDs */
static int CronTaskStartTimeout = 10000; /* maximum connection time */
static const int MaxWait = 1000; /* maximum time in ms that poll() can block */
static bool RebootJobsScheduled = false;


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

	DefineCustomBoolVariable(
		"cron.log_statement",
		gettext_noop("Log all cron statements prior to execution."),
		NULL,
		&CronLogStatement,
		true,
		PGC_POSTMASTER,
		GUC_SUPERUSER_ONLY,
		NULL, NULL, NULL);

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 1;
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

	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}
}


/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reload the cron jobs.
 */
static void
pg_cron_sighup(SIGNAL_ARGS)
{
	CronJobCacheValid = false;

	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}
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

	CronLoopContext = AllocSetContextCreate(CurrentMemoryContext,
											"pg_cron loop context",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);

	InitializeJobMetadataCache();
	InitializeTaskStateHash();

	ereport(LOG, (errmsg("pg_cron scheduler started")));

	MemoryContextSwitchTo(CronLoopContext);

	while (!got_sigterm)
	{
		List *taskList = NIL;
		TimestampTz currentTime = 0;

		AcceptInvalidationMessages();

		if (!CronJobCacheValid)
		{
			RefreshTaskHash();
		}

		taskList = CurrentTaskList();
		currentTime = GetCurrentTimestamp();

		StartAllPendingRuns(taskList, currentTime);

		WaitForCronTasks(taskList);
		ManageCronTasks(taskList, currentTime);

		MemoryContextReset(CronLoopContext);
	}

	ereport(LOG, (errmsg("pg_cron scheduler shutting down")));

	proc_exit(0);
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
		int rc = 0;
		int waitFlags = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT;

		/* nothing to do, wait for new jobs */
		rc = WaitLatch(MyLatch, waitFlags, MaxWait);

		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
		{
			/* postmaster died and we should bail out immediately */
			proc_exit(1);
		}
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
				RemoveTask(jobId);
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

			if (CronLogStatement)
			{
				char *command = cronJob->command;

				ereport(LOG, (errmsg("cron job %ld starting: %s",
									 jobId, command)));
			}

			connection = PQconnectStartParams(keywordArray, valueArray, false);
			PQsetnonblocking(connection, 1);

			connectionStatus = PQstatus(connection);
			if (connectionStatus == CONNECTION_BAD)
			{
				/* make sure we call PQfinish on the connection */
				task->connection = connection;

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
					case PGRES_COMMAND_OK:
					{
						if (CronLogStatement)
						{
							char *cmdStatus = PQcmdStatus(result);
							char *cmdTuples = PQcmdTuples(result);

							ereport(LOG, (errmsg("cron job %ld completed: %s %s",
												 jobId, cmdStatus, cmdTuples)));
						}

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

					case PGRES_TUPLES_OK:
					case PGRES_EMPTY_QUERY:
					case PGRES_SINGLE_TUPLE:
					case PGRES_NONFATAL_ERROR:
					default:
					{
						if (CronLogStatement)
						{
							int tupleCount = PQntuples(result);
							char *rowString = ngettext("row", "rows",
													   tupleCount);

							ereport(LOG, (errmsg("cron job %ld completed: "
												 "%d %s",
												 jobId, tupleCount,
												 rowString)));
						}

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
				RemoveTask(jobId);
			}

			if (task->errorMessage != NULL)
			{
				ereport(LOG, (errmsg("cron job %ld %s",
									 jobId, task->errorMessage)));
			}
			else
			{
				ereport(LOG, (errmsg("cron job %ld failed", jobId)));
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
