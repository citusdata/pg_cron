/*-------------------------------------------------------------------------
 *
 * cron_job.h
 *	  definition of the relation that holds cron jobs (cron.job).
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CRON_JOB_H
#define CRON_JOB_H


/* ----------------
 *		cron_job definition.
 * ----------------
 */
typedef struct FormData_cron_job
{
	int64 jobId;
#ifdef CATALOG_VARLEN
	text schedule;
	text command;
	text nodeName;
	int nodePort;
	text database;
	text userName;
	bool active;
#endif
} FormData_cron_job;

/* ----------------
 *      Form_cron_jobs corresponds to a pointer to a tuple with
 *      the format of cron_job relation.
 * ----------------
 */
typedef FormData_cron_job *Form_cron_job;

/* ----------------
 *      compiler constants for cron_job
 * ----------------
 */
#define Natts_cron_job 8
#define Anum_cron_job_jobid 1
#define Anum_cron_job_schedule 2
#define Anum_cron_job_command 3
#define Anum_cron_job_nodename 4
#define Anum_cron_job_nodeport 5
#define Anum_cron_job_database 6
#define Anum_cron_job_username 7
#define Anum_cron_job_active 8


#endif /* CRON_JOB_H */
