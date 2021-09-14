/* pg_cron--1.3--1.4.sql */

CREATE FUNCTION cron.alter_job(job_id bigint,
								schedule text default null,
								command text default null,
								database text default null,
								username text default null,
								active boolean default null)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', $$cron_alter_job$$;

COMMENT ON FUNCTION cron.alter_job(bigint,text,text,text,text,boolean)
IS 'Alter the job identified by job_id. Any option left as NULL will not be modified.';

/* admin should decide whether alter_job is safe by explicitly granting execute */
REVOKE ALL ON FUNCTION cron.alter_job(bigint,text,text,text,text,boolean) FROM public;

CREATE FUNCTION cron.schedule(job_name text,
								schedule text,
								command text,
								database text default null,
								username text default null,
								active boolean default 'true')
RETURNS bigint
LANGUAGE C
AS 'MODULE_PATHNAME', $$cron_schedule_named$$;

COMMENT ON FUNCTION cron.schedule(text,text,text,text,text,boolean)
IS 'schedule a pg_cron job';
