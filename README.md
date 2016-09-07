[![Citus Banner](/github-banner.png)](https://www.citusdata.com/)

[![Slack Status](http://slack.citusdata.com/badge.svg)](https://slack.citusdata.com)

## What is pg_cron?

pg_cron is a simple cron-based job scheduler for PostgreSQL runs inside the database. It uses the same syntax and much of the same code as regular cron, but it allows you to schedule PostgreSQL commands directly from the database:

```sql
-- Delete old data on Saturday at 3:30am (GMT)
SELECT cron.schedule('30 3 * * 6', $$DELETE FROM events WHERE event_time < now() - interval '1 week'$$);
 schedule
----------
       42

-- Vacuum every day at 10:00am (GMT)
SELECT cron.schedule('0 10 * * *', 'VACUUM');
 schedule
----------
       43
    
-- Stop scheduling a job
SELECT cron.unschedule(43);
 unschedule
------------
          t
```

pg_cron can run multiple jobs in parallel, but it runs at most one instance of a job at a time. If a second run is supposed to start before the first one finishes, then the second run is queued and started as soon as the first run completes.

The schedule uses the standard cron syntax, in which * means "run every time period", and a specific number means "but only at this time":

```
 ┌───────────── min (0 - 59)
 │ ┌────────────── hour (0 - 23)
 │ │ ┌─────────────── day of month (1 - 31)
 │ │ │ ┌──────────────── month (1 - 12)
 │ │ │ │ ┌───────────────── day of week (0 - 6) (0 to 6 are Sunday to
 │ │ │ │ │                  Saturday, or use names; 7 is also Sunday)
 │ │ │ │ │
 │ │ │ │ │
 * * * * * 
```

An easy way to create a cron schedule is: [crontab.guru](http://crontab.guru/).

## Setting up pg_cron

You can install pg_cron by building it from source:

```bash
git clone https://github.com/citusdata/pg_cron.git
cd pg_cron
PATH=/usr/local/pgsql/bin/:$PATH make
sudo PATH=/usr/local/pgsql/bin/:$PATH make install
```

To start the pg_cron background worker when PostgreSQL starts, you need to add pg_cron to `shared_preload_libraries` in postgresql.conf and restart PostgreSQL. Note that pg_cron does not run any jobs as a long a server is in [hot standby](https://www.postgresql.org/docs/current/static/hot-standby.html) mode, but it automatically starts when the server is promoted.

```
# add to postgresql.conf:
shared_preload_libraries = 'pg_cron'
```

After restarting PostgreSQL, you can create the pg_cron functions and metadata tables using `CREATE EXTENSION pg_cron`. By default, the pg_cron background worker expects its metadata tables to be created in the "postgres" database. However, you can configure this by setting the `cron.database_name` configuration parameter in postgresql.conf.

```sql
-- run using psql:
CREATE EXTENSION pg_cron;
```

Internally, pg_cron uses libpq to open a new connection to the local database. It may be necessary to enable `trust` authentication for connections coming from localhost in [pg_hba.conf](https://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html). Alternatively, you can create a [.pgpass file](https://www.postgresql.org/docs/current/static/libpq-pgpass.html) in the working directory of the database server.

## Advanced usage

Since pg_cron uses libpq, you can also run periodic jobs on other machines. This can be especially useful when you are using the [Citus extension](https://www.citusdata.com/product) to distribute tables across many PostgreSQL servers and need to run periodic jobs across all of them. 

If you are superuser, then you can manually modify the cron.job table and use custom values for nodename and nodeport to connect to a different machine:

```sql
INSERT INTO cron.job (schedule, command, nodename, nodeport, database, username)
VALUES ('0 4 * * *', 'VACUUM', 'worker-node-1', 5432, 'postgres', 'marco');
```

You can use [.pgpass](https://www.postgresql.org/docs/current/static/libpq-pgpass.html) to allow pg_cron to authenticate with the remote server.
