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


/* global settings */
extern char *CronTableDatabaseName;
#if (PG_VERSION_NUM < 110000) || (PG_VERSION_NUM >= 120000)
#define PgAllocSetContextCreate AllocSetContextCreate
#else
#define PgAllocSetContextCreate AllocSetContextCreateExtended
#endif

#endif
