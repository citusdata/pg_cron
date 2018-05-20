# src/test/modules/pg_cron/Makefile

EXTENSION = pg_cron
EXTVERSION = 1.1

DATA_built = $(EXTENSION)--$(EXTVERSION).sql $(EXTENSION)--1.0.sql
DATA = $(wildcard $(EXTENSION)--*--*.sql)
REGRESS = pg_cron-test 

# compilation configuration
MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))
ifeq ($(CC),gcc)
    PG_CPPFLAGS = -std=c99 -Wall -Wextra -Werror -Wno-unused-parameter -Wno-maybe-uninitialized -Wno-implicit-fallthrough -Iinclude -I$(libpq_srcdir)
else
    PG_CPPFLAGS = -std=c99 -Wall -Wextra -Werror -Wno-unused-parameter -Wno-implicit-fallthrough -Iinclude -I$(libpq_srcdir)
endif
SHLIB_LINK = $(libpq)
EXTRA_CLEAN += $(addprefix src/,*.gcno *.gcda) # clean up after profiling runs

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(EXTENSION)--1.0.sql: $(EXTENSION).sql $(EXTENSION)--0.1--1.0.sql
	cat $^ > $@
$(EXTENSION)--1.1.sql: $(EXTENSION).sql $(EXTENSION)--1.0--1.1.sql
	cat $^ > $@
