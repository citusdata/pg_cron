### pg_cron v1.0.1 (June 30, 2017) ###

* Fixes a memory leak that occurs when a connection fails immediately
* Fixes a memory leak due to switching memory context when loading metadata
* Fixes a segmentation fault that can occur when using an error message after PQclear

### pg_cron v1.0.0 (January 27, 2017) ###

* Use WaitLatch instead of pg_usleep when there are no tasks

### pg_cron v1.0.0-rc.1 (December 14, 2016) ###

* Initial 1.0 candidate
