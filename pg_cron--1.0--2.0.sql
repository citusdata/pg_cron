/* pg_cron 2.0 catalog changes */

ALTER TABLE cron.job ALTER COLUMN nodename SET DEFAULT '';
ALTER TABLE cron.job ADD COLUMN jobname name;

CREATE FUNCTION cron.schedule(job_name name, schedule text, command text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_schedule_named$$;
COMMENT ON FUNCTION cron.schedule(name,text,text)
    IS 'schedule a pg_cron job';

CREATE FUNCTION cron.unschedule(job_name name)
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_unschedule_named$$;
COMMENT ON FUNCTION cron.unschedule(name)
    IS 'unschedule a pg_cron job';
