/*-------------------------------------------------------------------------
 *
 * pg_cron.h
 *	  definition of pg_cron data types
 *
 * Copyright (c) 2010-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_CRON_H
#define PG_CRON_H

#include "cron_job.h"
#include "libpq-fe.h"
#include "utils/timestamp.h"

typedef enum
{
	CLOCK_JUMP_BACKWARD = 0,
	CLOCK_PROGRESSED = 1,
	CLOCK_JUMP_FORWARD = 2,
	CLOCK_CHANGE = 3
} ClockProgress;

typedef enum
{
	CRON_TASK_WAITING = 0,
	CRON_TASK_START = 1,
	CRON_TASK_CONNECTING = 2,
	CRON_TASK_SENDING = 3,
	CRON_TASK_RUNNING = 4,
	CRON_TASK_RECEIVING = 5,
	CRON_TASK_DONE = 6,
	CRON_TASK_ERROR = 7
} CronTaskState;

typedef struct CronJob
{
	int64 jobId;
	char *scheduleText;
	entry schedule;
	char *command;
	char *nodeName;
	int nodePort;
	char *database;
	char *userName;
} CronJob;

typedef struct CronTask
{
	int64 jobId;
	int64 runId;
	CronTaskState state;
	int pendingRunCount;
	PGconn *connection;
	PostgresPollingStatusType pollingStatus;
	TimestampTz startDeadline;
	bool isSocketReady;
	bool isActive;
	char *errorMessage;
} CronTask;

#endif
