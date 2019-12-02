CREATE EXTENSION pg_cron VERSION '1.0';
SELECT extversion FROM pg_extension WHERE extname='pg_cron';
ALTER EXTENSION pg_cron UPDATE TO '1.1';
SELECT extversion FROM pg_extension WHERE extname='pg_cron';

-- Vacuum every day at 10:00am (GMT)
SELECT cron.schedule('0 10 * * *', 'VACUUM');

-- Stop scheduling a job
SELECT cron.unschedule(1);

-- Invalid input: input too long
SELECT cron.schedule(repeat('a', 1000), '');

DROP EXTENSION pg_cron;
