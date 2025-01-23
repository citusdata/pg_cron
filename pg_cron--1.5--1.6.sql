DROP FUNCTION IF EXISTS cron.shutdown();
CREATE FUNCTION cron.shutdown()
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_shutdown$$;
COMMENT ON FUNCTION cron.shutdown()
    IS 'shutdown pg_cron';
