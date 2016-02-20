CREATE SCHEMA cron

	CREATE TABLE jobs (
		job_name text not null,
		cron_string not null,
		query text not null,
		connection_string text not null,
		PRIMARY KEY (job_name)
	)

	CREATE TABLE results (
		id bigint primary key default nextval('task_id_sequence'),
		job_name text not null,
		start_time timestamptz,
		end_time timestamptz,
		status int not null,
		output text
	)

	CREATE SEQUENCE task_id_sequence NO CYCLE;

CREATE FUNCTION cron.schedule(text,text,text,text)
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$pg_cron_schedule$$;
COMMENT ON FUNCTION cron.schedule(text,text,text,text)
    IS 'schedule a pg_cron job';

CREATE FUNCTION cron.unschedule(text)
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$pg_cron_schedule$$;
COMMENT ON FUNCTION cron.unschedule(text)
    IS 'unschedule a pg_cron job';
