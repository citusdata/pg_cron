CREATE SCHEMA cron;
GRANT USAGE ON SCHEMA cron TO public;

CREATE SEQUENCE cron.jobid_seq;
GRANT USAGE ON SEQUENCE cron.jobid_seq TO public;

CREATE TABLE cron.job (
	jobid bigint primary key default nextval('cron.jobid_seq'),
	schedule text not null,
	command text not null,
	nodename text not null default 'localhost',
	nodeport int not null default inet_server_port(),
	database text not null default current_database(),
	username text not null default current_user
);
GRANT SELECT ON cron.job TO public;
ALTER TABLE cron.job ENABLE ROW LEVEL SECURITY;
CREATE POLICY cron_job_policy ON cron.job USING (username = current_user);

CREATE FUNCTION cron.schedule(schedule text, command text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_schedule$$;
COMMENT ON FUNCTION cron.schedule(text,text)
    IS 'schedule a pg_cron job';

CREATE FUNCTION cron.unschedule(job_id bigint)
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_unschedule$$;
COMMENT ON FUNCTION cron.unschedule(bigint)
    IS 'unschedule a pg_cron job';

CREATE FUNCTION cron.job_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$cron_job_cache_invalidate$$;
COMMENT ON FUNCTION cron.job_cache_invalidate()
    IS 'invalidate job cache';

CREATE TRIGGER cron_job_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON cron.job
    FOR STATEMENT EXECUTE PROCEDURE cron.job_cache_invalidate();
