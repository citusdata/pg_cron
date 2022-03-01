/* pg_cron--1.4.1--1.5.sql */

CREATE TABLE cron.lt_job_ext (jobid bigint,
								jobname text,
								username text,
								mode text,
								timezone text
);

GRANT SELECT ON cron.lt_job_ext TO public;
ALTER TABLE cron.lt_job_ext ENABLE ROW LEVEL SECURITY;
CREATE POLICY lt_job_ext_policy ON cron.lt_job_ext USING (username = current_user);

CREATE UNIQUE INDEX jobid_username_idx ON cron.lt_job_ext (jobid, username);
ALTER TABLE cron.lt_job_ext ADD CONSTRAINT jobid_username_uniq UNIQUE USING INDEX jobid_username_idx;

CREATE VIEW cron.lt_job AS
select cron.job.jobid, cron.job.jobname, command, schedule, nodename, nodeport, database, cron.job.username, active,
       mode, timezone from cron.job, cron.lt_job_ext where cron.job.jobid = cron.lt_job_ext.jobid and cron.job.active = true;

CREATE FUNCTION cron.schedule_mode_zone(job_name text, schedule text, command text, single_mode text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_schedule_named_mode$$;
COMMENT ON FUNCTION cron.schedule_mode_zone(text,text,text,text)
    IS 'schedule a pg_cron job';

CREATE FUNCTION cron.schedule_mode_zone(job_name text, schedule text, command text, single_mode text, tmzone text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_schedule_named_mode_zone$$;
COMMENT ON FUNCTION cron.schedule_mode_zone(text,text,text,text,text)
    IS 'schedule a pg_cron job';
