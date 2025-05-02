/* no SQL changes in 1.6 */

/* Ensure the extension can be installed as trusted extension by non-superuser*/
DO $$
DECLARE
    pg_version_num TEXT;
    major_version INT;
BEGIN
    -- Get the pg version
    SELECT current_setting('server_version_num') INTO pg_version_num;
    major_version := pg_version_num::INT;

    -- Starting from pg13, if the extension script contains the string @extowner@, 
    -- that string is replaced with the (suitably quoted) name of the user calling 
    -- CREATE EXTENSION or ALTER EXTENSION.
    IF major_version >= 130000 THEN
        ALTER SCHEMA cron OWNER TO @extowner@;

        -- make extension-owner as the owner of the tables
        ALTER TABLE cron.job OWNER TO @extowner@;
        ALTER TABLE cron.job_run_details OWNER TO @extowner@;

        -- extension-owner should be able to read and write to the tables
        GRANT ALL ON ALL TABLES IN SCHEMA cron TO @extowner@;
        GRANT ALL ON ALL SEQUENCES IN SCHEMA cron TO @extowner@;

        -- extension-owner should own the basic functions which are safe.
        -- schedule_in_database is removed from this list, 
        -- as SUPERUSER can grant access to it explicitly if needed.
        GRANT ALL ON FUNCTION cron.alter_job(bigint,text,text,text,text,boolean) TO @extowner@;
        GRANT ALL ON FUNCTION cron.job_cache_invalidate() TO @extowner@;
        GRANT ALL ON FUNCTION cron.schedule(text,text) TO @extowner@;
        GRANT ALL ON FUNCTION cron.schedule(text,text,text) TO @extowner@;
        GRANT ALL ON FUNCTION cron.unschedule(bigint) TO @extowner@;
        GRANT ALL ON FUNCTION cron.unschedule(text) TO @extowner@;

        ALTER FUNCTION cron.alter_job(bigint,text,text,text,text,boolean) OWNER TO @extowner@;
        ALTER FUNCTION cron.job_cache_invalidate() OWNER TO @extowner@;
        ALTER FUNCTION cron.schedule(text,text) OWNER TO @extowner@;
        ALTER FUNCTION cron.schedule(text,text,text) OWNER TO @extowner@;
        ALTER FUNCTION cron.unschedule(bigint) OWNER TO @extowner@;
        ALTER FUNCTION cron.unschedule(text) OWNER TO @extowner@;
    END IF;
END $$;
