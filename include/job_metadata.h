/*-------------------------------------------------------------------------
 *
 * job_metadata.h
 *	  definition of job metadata functions
 *
 * Copyright (c) 2010-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef JOB_METADATA_H
#define JOB_METADATA_H


#include "nodes/pg_list.h"


/* job metadata data structure */
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
	bool active;
} CronJob;


/* global settings */
extern char *CronHost;
extern bool CronJobCacheValid;


/* functions for retrieving job metadata */
extern void InitializeJobMetadataCache(void);
extern void ResetJobMetadataCache(void);
extern List * LoadCronJobList(void);
extern CronJob * GetCronJob(int64 jobId);


#endif
