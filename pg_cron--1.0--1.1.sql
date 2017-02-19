/* pg_cron--1.0--1.1.sql */

CREATE FUNCTION cron.schedule(schedule text, command text, database text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_schedule$$;
COMMENT ON FUNCTION cron.schedule(text,text,text)
    IS 'schedule a pg_cron job in a specific database';
