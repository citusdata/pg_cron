# src/test/modules/pg_cron/Makefile

EXTENSION = pg_cron
EXTVERSION = 1.1

DATA_built = $(EXTENSION)--$(EXTVERSION).sql
DATA = $(wildcard $(EXTENSION)--*--*.sql)

# compilation configuration
MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))
PG_CPPFLAGS = -std=c99 -Wall -Wextra -Werror -Wno-unused-parameter -Iinclude -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)
EXTRA_CLEAN += $(addprefix src/,*.gcno *.gcda) # clean up after profiling runs

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(EXTENSION)--1.0.sql: $(EXTENSION).sql $(EXTENSION)--0.1--1.0.sql
	cat $^ > $@
$(EXTENSION)--1.1.sql: $(EXTENSION).sql $(EXTENSION)--1.0--1.1.sql
	cat $^ > $@
