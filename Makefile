# src/test/modules/pg_cron/Makefile

EXTENSION = pg_cron

DATA_built = $(EXTENSION)--1.0.sql
DATA = $(wildcard $(EXTENSION)--*--*.sql)

REGRESS_OPTS =--temp-config=./pg_cron.conf --temp-instance=./tmp_check
REGRESS = pg_cron-test 

# compilation configuration
MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))
ifeq ($(CC),gcc)
    PG_CPPFLAGS = -std=c99 -Wall -Wextra -Werror -Wno-unused-parameter -Wno-uninitialized -Wno-implicit-fallthrough -Iinclude -I$(libpq_srcdir)
else
    PG_CPPFLAGS = -std=c99 -Wall -Wextra -Werror -Wno-unused-parameter -Wno-implicit-fallthrough -Iinclude -I$(libpq_srcdir)
endif
SHLIB_LINK = $(libpq)
EXTRA_CLEAN += $(addprefix src/,*.gcno *.gcda) # clean up after profiling runs

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(EXTENSION)--1.0.sql: $(EXTENSION).sql
	cat $^ > $@
