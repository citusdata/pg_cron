/*-------------------------------------------------------------------------
 *
 * include/job_metadata.h
 *
 * Declarations for public functions and types related to job metadata.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "postgres.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "nodes/pg_list.h"


/*
 * CronJob represents a periodic job.
 */
typedef struct CronJob
{
	char *jobName;
	char *cronString;
	char *query;
	char *connectionString;

} CronJob;


extern List * LoadCronJobList(void);
extern void LogCronTaskResult(char *jobName, int taskStatus);
