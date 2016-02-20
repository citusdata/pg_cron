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

/* these are internal headers */
#include "job_metadata.h"

/* these are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "sys/time.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "tcop/utility.h"


typedef enum
{
	CRON_TASK_INITIAL = 0,
	CRON_TASK_CONNECTING = 1,
	CRON_TASK_SENDING = 2,
	CRON_TASK_RUNNING = 3,
	CRON_TASK_RECEIVING = 4
	CRON_TASK_OK = 5,
	CRON_TASK_ERROR = 6,
	CRON_TASK_WAITING = 7
	
} CronTaskState;

typedef struct CronTask
{
	CronJob *job;
	CronTaskState state;
	PGconn *connection;
	bool readyToPoll;
	PostgresPollingStatusType pollingStatus;
	struct timeval nextEventTime;

} CronTask;


void _PG_init(void);
static void PgOctopusWorkerMain(Datum arg);
static List * CreateCronTasks(List *cronJobList);
static CronTask * CreateCronTask(CronJob *cronJob);
static void DoCronTasks(List *taskList);
static void ManageCronTask(CronTask *task, struct timeval currentTime);
static int WaitForEvent(List *taskList);
static int CompareTimes(struct timeval *leftTime, struct timeval *rightTime);
static int SubtractTimes(struct timeval base, struct timeval subtract);
static struct timeval AddTimeMillis(struct timeval base, uint32 additionalMs);


PG_MODULE_MAGIC;


/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;


/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
	{
		return;
	}

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = PgOctopusWorkerMain;
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
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pg_cron_sighup(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * PgOctopusWorkerMain is the main entry-point for the background worker
 * that performs tasks.
 */
static void
PgOctopusWorkerMain(Datum arg)
{
	MemoryContext taskContext = NULL;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_cron_sighup);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, pg_cron_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	taskContext = AllocSetContextCreate(CurrentMemoryContext,
											   "CronTask context",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextSwitchTo(taskContext);

	elog(LOG, "pg_cron scheduler started");

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		List *cronJobList = NIL;
		List *taskList = NIL;

		cronJobList = LoadCronJobList();
		taskList = CreateCronTasks(cronJobList);

		DoCronTasks(taskList);

		MemoryContextReset(taskContext);

		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	elog(LOG, "pg_cron scheduler exiting");

	proc_exit(0);
}


/*
 * CreateCronTasks creates a list of tasks from a list of job
 * descriptions.
 */
static List *
CreateCronTasks(List *cronJobList)
{
	List *taskList = NIL;
	ListCell *cronJobCell = NULL;

	foreach(cronJobCell, cronJobList)
	{
		CronJob *cronJob = (CronJob *) lfirst(cronJobCell);
		CronTask *task = CreateCronTask(cronJob);
		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * CreateCronTask creates a task from a job description.
 */
static CronTask *
CreateCronTask(CronJob *cronJob)
{
	CronTask *task = NULL;
	struct timeval invalidTime = {0, 0};

	task = palloc0(sizeof(CronTask));
	task->job = cronJob;
	task->state = CRON_TASK_INITIAL;
	task->connection = NULL;
	task->nextEventTime = invalidTime;

	return task;
}


/*
 * DoCronTasks performs the given tasks.
 */
static void
DoCronTasks(List *taskList)
{
	while (!got_sigterm)
	{
		int pendingCheckCount = 0;
		struct timeval currentTime = {0, 0};
		ListCell *taskCell = NULL;

		gettimeofday(&currentTime, NULL);

		foreach(taskCell, taskList)
		{
			CronTask *task = (CronTask *) lfirst(taskCell);

			ManageCronTask(task, currentTime);

			if (task->state != CRON_TASK_OK &&
				task->state != CRON_TASK_DEAD)
			{
				pendingCheckCount++;
			}
		}
		if (pendingCheckCount == 0)
		{
			break;
		}

		WaitForEvent(taskList);
	}
}


/*
 * WaitForEvent sleeps until a time-based or I/O event occurs in any of the jobs. 
 */
static int
WaitForEvent(List *taskList)
{
	ListCell *taskCell = NULL;
	int taskCount = list_length(taskList);
	int taskIndex = 0;
	struct timeval currentTime = {0, 0};
	struct timeval nextEventTime = {0, 0};
	int pollTimeout = 0;
	struct pollfd *pollFDs = NULL;
	int pollResult = 0;

	pollFDs = (struct pollfd *) palloc0(taskCount * sizeof(struct pollfd));

	gettimeofday(&currentTime, NULL);

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);
		struct pollfd *pollFileDescriptor = NULL;
		pollFileDescriptor = &pollFDs[taskIndex];

		if (task->state == CRON_TASK_WAITING)
		{
			bool hasTimeout = task->nextEventTime.tv_sec != 0;

			if (hasTimeout &&
				(nextEventTime.tv_sec == 0 ||
				 CompareTimes(&task->nextEventTime, &nextEventTime) < 0))
			{
				nextEventTime = task->nextEventTime;
			}
		}

		if (task->state == CRON_TASK_CONNECTING
			task->state == CRON_TASK_SENDING
			task->state == CRON_TASK_RUNNING
			task->state == CRON_TASK_RECEIVING)
		{
			PGconn *connection = task->connection;
			int pollEventMask = 0;

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
			pollFileDescriptor->fd = -1;
			pollFileDescriptor->events = 0;
		}

		pollFileDescriptor->revents = 0;

		taskIndex++;
	}

	pollTimeout = SubtractTimes(nextEventTime, currentTime);
	pollResult = poll(pollFDs, taskCount, pollTimeout);

	if (pollResult < 0)
	{
		return STATUS_ERROR;
	}

	taskIndex = 0;

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);
		struct pollfd *pollFileDescriptor = &pollFDs[taskIndex];

		task->readyToPoll = pollFileDescriptor->revents & pollFileDescriptor->events;

		taskIndex++;
	}

	return 0;
}


/*
 * ManageCronTask proceeds the task state machine.
 */
static void
ManageCronTask(CronTask *task, struct timeval currentTime)
{
	CronTaskState checkState = task->state;
	CronJob *cronJob = task->job;
	
	switch (checkState)
	{
		case CRON_TASK_INITIAL:
		{
			PGconn *connection = NULL;
			ConnStatusType connStatus = CONNECTION_BAD;
			
			connection = PQconnectStart(cronJob->connectionString);
			connStatus = PQstatus(connection);
			if (connStatus == CONNECTION_BAD)
			{
				PQfinish(connection);

				LogTaskResult(cronJob->jobName, 1);

				task->connection = NULL;
				task->pollingStatus = PGRES_POLLING_FAILED;
				task->state = CRON_TASK_DEAD;
			}
			else
			{
				struct timeval timeoutTime = {0, 0};

				timeoutTime = AddTimeMillis(currentTime, CronTaskTimeout);

				task->nextEventTime = timeoutTime;
				task->connection = connection;
				task->pollingStatus = PGRES_POLLING_WRITING;
				task->state = CRON_TASK_CONNECTING;
			}

			break;
		}

		case CRON_TASK_CONNECTING:
		{
			PGconn *connection = task->connection;
			PostgresPollingStatusType pollingStatus = PGRES_POLLING_FAILED;

			if (CompareTimes(&task->nextEventTime, &currentTime) < 0)
			{
				PQfinish(connection);

				LogTaskResult(cronJob->jobName, 1);

				task->connection = NULL;
				task->pollingStatus = pollingStatus;
				task->state = CRON_TASK_DEAD;
				break;
			}

			if (!task->readyToPoll)
			{
				break;
			}

			pollingStatus = PQconnectPoll(connection);
			if (pollingStatus == PGRES_POLLING_FAILED)
			{
				PQfinish(connection);

				LogTaskResult(cronJob->jobName, 1);

				task->connection = NULL;
				task->state = CRON_TASK_DEAD;
			}
			else if (pollingStatus == PGRES_POLLING_OK)
			{
				PQfinish(connection);

				LogTaskResult(cronJob->jobName, 0);

				task->connection = NULL;
				task->state = CRON_TASK_OK;
			}
			else
			{
				/* still connecting */
			}

			task->pollingStatus = pollingStatus;

			break;
		}

		case CRON_TASK_DEAD:
		case CRON_TASK_OK:
		default:
		{
			/* task is done */
		}

	}
}


/*
 * CompareTime compares two timeval structs.
 *
 * If leftTime < rightTime, return -1
 * If leftTime > rightTime, return 1
 * else, return 0
 */
static int
CompareTimes(struct timeval *leftTime, struct timeval *rightTime)
{
	int compareResult = 0;
	
	if (leftTime->tv_sec < rightTime->tv_sec)
	{
		compareResult = -1;
	}
	else if (leftTime->tv_sec > rightTime->tv_sec)
	{
		compareResult = 1;
	}
	else if (leftTime->tv_usec < rightTime->tv_usec)
	{
		compareResult = -1;
	}
	else if (leftTime->tv_usec > rightTime->tv_usec)
	{
		compareResult = 1;
	}
	else
	{
		compareResult = 0;
	}

	return compareResult;
}

/*
 * SubtractTimes subtracts the struct timeval values y from x,
 * returning the result in milliseconds.
 *
 * From:
 * http://www.gnu.org/software/libc/manual/html_node/Elapsed-Time.html
 */
static int
SubtractTimes(struct timeval x, struct timeval y)
{
	int differenceMs = 0;

	/* Perform the carry for the later subtraction by updating y. */
	if (x.tv_usec < y.tv_usec)
	{
		int nsec = (y.tv_usec - x.tv_usec) / 1000000 + 1;
		y.tv_usec -= 1000000 * nsec;
		y.tv_sec += nsec;
	}

	if (x.tv_usec - y.tv_usec > 1000000)
	{
		int nsec = (x.tv_usec - y.tv_usec) / 1000000;
		y.tv_usec += 1000000 * nsec;
		y.tv_sec -= nsec;
	}

	differenceMs += 1000 * (x.tv_sec - y.tv_sec);
	differenceMs += (x.tv_usec - y.tv_usec) / 1000;

	return differenceMs;
}


/*
 * AddTimeMillis adds additionalMs milliseconds to a timeval.
 */
static struct timeval
AddTimeMillis(struct timeval base, uint32 additionalMs)
{
	struct timeval result = {0, 0};

	result.tv_sec = base.tv_sec + additionalMs / 1000;
	result.tv_usec = base.tv_usec + (additionalMs % 1000) * 1000;

	return result;
}

