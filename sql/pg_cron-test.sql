CREATE EXTENSION pg_cron VERSION '1.0';
SELECT extversion FROM pg_extension WHERE extname='pg_cron';
ALTER EXTENSION pg_cron UPDATE TO '1.3';
SELECT extversion FROM pg_extension WHERE extname='pg_cron';

-- Vacuum every day at 10:00am (GMT)
SELECT cron.schedule('0 10 * * *', 'VACUUM');

-- Stop scheduling a job
SELECT cron.unschedule(1);


-- Invalid input: input too long
SELECT cron.schedule(repeat('a', 1000), '');

-- Try to update pg_cron on restart
SELECT cron.schedule('@restar', 'ALTER EXTENSION pg_cron UPDATE');
SELECT cron.schedule('@restart', 'ALTER EXTENSION pg_cron UPDATE');

-- Vacuum every day at 10:00am (GMT)
SELECT cron.schedule('myvacuum', '0 10 * * *', 'VACUUM');

SELECT jobid, jobname, schedule, command FROM cron.job ORDER BY jobid;

-- Make that 11:00am (GMT)
SELECT cron.schedule('myvacuum', '0 11 * * *', 'VACUUM');

SELECT jobid, jobname, schedule, command FROM cron.job ORDER BY jobid;

-- Make that VACUUM FULL
SELECT cron.schedule('myvacuum', '0 11 * * *', 'VACUUM FULL');

SELECT jobid, jobname, schedule, command FROM cron.job ORDER BY jobid;

-- Stop scheduling a job
SELECT cron.unschedule('myvacuum');

SELECT jobid, jobname, schedule, command FROM cron.job ORDER BY jobid;

DROP EXTENSION pg_cron;
