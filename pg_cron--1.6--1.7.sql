DROP FUNCTION IF EXISTS cron.shutdown();
CREATE FUNCTION cron.shutdown()
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_shutdown$$;
COMMENT ON FUNCTION cron.shutdown()
    IS 'shutdown pg_cron';
REVOKE ALL ON FUNCTION cron.shutdown() FROM PUBLIC;

DROP FUNCTION IF EXISTS cron.startup();
CREATE FUNCTION cron.startup()
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_startup$$;
COMMENT ON FUNCTION cron.startup()
    IS 'starts up pg_cron after it was shut down';
REVOKE ALL ON FUNCTION cron.startup() FROM PUBLIC;
