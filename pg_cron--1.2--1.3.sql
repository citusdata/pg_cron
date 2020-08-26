/* pg_cron--1.2--1.3.sql */

CREATE SEQUENCE cron.runid_seq;
CREATE TABLE cron.job_run_details (
	jobid bigint,
	runid bigint primary key default nextval('cron.runid_seq'),
	job_pid integer,
	database text,
	username text,
	command text,
	status text,
	return_message text,
	start_time timestamptz,
	end_time timestamptz
);

GRANT SELECT ON cron.job_run_details TO public;
ALTER TABLE cron.job_run_details ENABLE ROW LEVEL SECURITY;
CREATE POLICY cron_job_run_details_policy ON cron.job_run_details USING (username = current_user);

SELECT pg_catalog.pg_extension_config_dump('cron.job_run_details', '');
SELECT pg_catalog.pg_extension_config_dump('cron.runid_seq', '');
