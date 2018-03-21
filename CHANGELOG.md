### pg_cron v1.1.0 (March 22, 2018) ###

* Add new 'active' column on cron.job table to enable or disable job(s).
* Added a regression test, simply run 'make installcheck'
* Increased pg_cron version to 1.1

### pg_cron v1.0.2 (October 6, 2017) ###

* PostgreSQL 10 support
* Restrict the maximum number of concurrent tasks
* Ensure table locks on cron.job are kept after schedule/unschedule

### pg_cron v1.0.1 (June 30, 2017) ###

* Fixes a memory leak that occurs when a connection fails immediately
* Fixes a memory leak due to switching memory context when loading metadata
* Fixes a segmentation fault that can occur when using an error message after PQclear

### pg_cron v1.0.0 (January 27, 2017) ###

* Use WaitLatch instead of pg_usleep when there are no tasks

### pg_cron v1.0.0-rc.1 (December 14, 2016) ###

* Initial 1.0 candidate
