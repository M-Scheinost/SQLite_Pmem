SRCS=$(wildcard *.c)
OBJS=$(SRCS:.c=.o)
HDRS=$(wildcard *.h)
TARGET=sqlite-bench

all:
	gcc $(shell pkg-config --cflags libpmem) -O2 -I. benchmark/bench.h benchmark/benchmark.c benchmark/histogram.c vfs/pmem_vfs.h vfs/pmem_vfs.c benchmark/main.c benchmark/random.c benchmark/raw.c sqlite/sqlite.c -lpthread -lpmem 