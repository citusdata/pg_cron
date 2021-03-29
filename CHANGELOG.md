### pg_cron v1.3.1 (March 29, 2021) ###

* Fixes a memory leak

### pg_cron v1.3.0 (September 30, 2020) ###

* Background worker support by Bertrand Drouvot
* Audit table support by Bertrand Drouvot
* PostgreSQL 13 support by Alexander Kukushkin
* Schedule jobs by name
* Fixes a bug that could cause cron.schedule to crash with long schedules
* Fixes a bug that could cause cron.schedule to get into an infinite loop
* Fixes a bug that caused overlapping runs not to start

### pg_cron v1.2.0 (August 30, 2019) ###

* PostgreSQL 12 support by dverite
* Fixes a bug that caused the cron.job table to not appear in pg_dump

### pg_cron v1.1.4 (April 4, 2019) ###

* Adds a cron.host setting to make the postgres host configurable
* Fixes a bug that could cause segmentation fault after cron.unschedule

### pg_cron v1.1.3 (November 15, 2018) ###

* Fixes a bug that causes pg_cron to run during pg_upgrade
* Fixes a bug that causes pg_cron to show up incorrectly in pg_stat_activity in PG11

### pg_cron v1.1.2 (July 10, 2018) ###

* PostgreSQL 11 support by dverite
* Fix a clang build error by kxjhlele

### pg_cron v1.1.1 (June 7, 2018) ###

* Fixed a bug that would cause new jobs to be created as inactive

### pg_cron v1.1.0 (March 22, 2018) ###

* Add new 'active' column on cron.job table to enable or disable job(s).
* Added a regression test, simply run 'make installcheck'
* Set relevant application_name in pg_stat_activity
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
