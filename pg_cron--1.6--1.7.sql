/* pg_cron--1.6--1.7.sql */

ALTER TABLE cron.job ADD COLUMN log_run boolean not null default 'true';

/*
 * cron.alter_job gains a log_run argument. DROP of the previous 6-argument
 * signature means any GRANT/REVOKE on
 * cron.alter_job(bigint,text,text,text,text,boolean) must be re-applied to
 * cron.alter_job(bigint,text,text,text,text,boolean,boolean) after upgrade.
 */

CREATE FUNCTION cron.schedule(job_name text,
                              schedule text,
                              command text,
                              log_run boolean)
RETURNS bigint
LANGUAGE C
AS 'MODULE_PATHNAME', $$cron_schedule_named$$;
COMMENT ON FUNCTION cron.schedule(text,text,text,boolean)
IS 'schedule a pg_cron job';

DROP FUNCTION cron.alter_job(bigint,text,text,text,text,boolean);
CREATE FUNCTION cron.alter_job(job_id bigint,
                                schedule text default null,
                                command text default null,
                                database text default null,
                                username text default null,
                                active boolean default null,
                                log_run boolean default null)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', $$cron_alter_job$$;

COMMENT ON FUNCTION cron.alter_job(bigint,text,text,text,text,boolean,boolean)
IS 'Alter the job identified by job_id. Any option left as NULL will not be modified.';

/* admin should decide whether alter_job is safe by explicitly granting execute */
REVOKE ALL ON FUNCTION cron.alter_job(bigint,text,text,text,text,boolean,boolean) FROM public;
