/*-------------------------------------------------------------------------
 *
 * src/job_metadata.c
 *
 * Implementation of functions related to job metadata.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "job_metadata.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"


/* human-readable names for addressing columns of job metadata */
#define TLIST_NUM_JOB_NAME 1
#define TLIST_NUM_CRON_STRING 2
#define TLIST_NUM_QUERY 3
#define TLIST_NUM_CONNECTION_STRING 4


static void StartSPITransaction(void);
extern CronJob * TupleToCronJob(HeapTuple heapTuple,
								TupleDesc tupleDescriptor);
static void EndSPITransaction(void);


/*
 * LoadCronJobList loads a list of periodic jobs.
 */
List *
LoadCronJobList(void)
{
	List *cronJobList = NIL;
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;
	StringInfoData query;

	MemoryContext upperContext = CurrentMemoryContext, oldContext = NULL;

	StartSPITransaction();

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT job_name, cron_string, query, connection_string "
					 "FROM cron.jobs");

	pgstat_report_activity(STATE_RUNNING, query.data);

	spiStatus = SPI_execute(query.data, false, 0);
	Assert(spiStatus == SPI_OK_SELECT);

	oldContext = MemoryContextSwitchTo(upperContext);

	for (uint32 rowNumber = 0; rowNumber < SPI_processed; rowNumber++)
	{
		HeapTuple heapTuple = SPI_tuptable->vals[rowNumber];
		CronJob *cronJob = TupleToCronJob(heapTuple,
										  SPI_tuptable->tupdesc);
		cronJobList = lappend(cronJobList, cronJob);
	}

	MemoryContextSwitchTo(oldContext);

	pgstat_report_activity(STATE_IDLE, NULL);

	EndSPITransaction();

	return cronJobList;
}


/*
 * TupleToCronJob constructs a job description from a heap tuple obtained
 * via SPI.
 */
CronJob *
TupleToCronJob(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	CronJob *cronJob = NULL;
	bool isNull[4] = {false, false, false, false};

	Datum jobNameDatum = SPI_getbinval(heapTuple, tupleDescriptor,
									   TLIST_NUM_JOB_NAME, &isNull[0]);
	Datum cronStringDatum = SPI_getbinval(heapTuple, tupleDescriptor,
										  TLIST_NUM_CRON_STRING, &isNull[1]);
	Datum queryDatum = SPI_getbinval(heapTuple, tupleDescriptor,
									 TLIST_NUM_QUERY, &isNull[2]);
	Datum connectionStringDatum = SPI_getbinval(heapTuple, tupleDescriptor,
												TLIST_NUM_CONNECTION_STRING,
												&isNull[3]);

	cronJob = palloc0(sizeof(CronJob));
	cronJob->jobName = TextDatumGetCString(jobNameDatum);
	cronJob->cronString = TextDatumGetCString(cronStringDatum);
	cronJob->query = TextDatumGetCString(queryDatum);
	cronJob->connectionString = TextDatumGetCString(connectionStringDatum);

	return cronJob;
}


/*
 * LogCronTaskResult logs the result of a cron task.
 */
void
LogCronTaskResult(char *jobName, int taskStatus)
{
	StringInfoData query;
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	StartSPITransaction();

	initStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO cron.results "
					 "(job_name, status) "
					 "VALUES (%s,%d) "
					 quote_literal_cstr(jobName),
					 taskStatus);

	pgstat_report_activity(STATE_RUNNING, query.data);

	spiStatus = SPI_execute(query.data, false, 0);
	Assert(spiStatus == SPI_OK_UPDATE);

	pgstat_report_activity(STATE_IDLE, NULL);

	EndSPITransaction();
}


/*
 * StartSPITransaction starts a transaction using SPI.
 */
static void
StartSPITransaction(void)
{
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
}


/*
 * EndSPITransaction finishes a transaction that was started using SPI.
 */
static void
EndSPITransaction(void)
{
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}
