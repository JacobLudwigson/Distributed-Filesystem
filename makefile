CC       := gcc
CFLAGS   := -Wall -Wextra -g
LDFLAGS  := 
DFSLDFLAGS := -pthread

DEBUG ?= 0
TARGETS  := dfs dfc

ifeq ($(DEBUG),1)
	CFLAGS += -g -fsanitize=address -O0
	LDFLAGS += -fsanitize=address
endif

all: $(TARGETS)

dfs: dfs.c
	$(CC) $(CFLAGS) $< -o $@ $(DFSLDFLAGS)

dfc: dfc.c
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -f $(TARGETS) *.o