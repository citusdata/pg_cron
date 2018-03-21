/* pg_cron--1.0--1.1.sql */

DO $$ 
BEGIN
    BEGIN
        ALTER TABLE cron.job ADD COLUMN active boolean not null default 'true';
    EXCEPTION
        WHEN duplicate_column THEN RAISE NOTICE 'column <active> already exists in <cron.job>.';
    END;
END;
$$

