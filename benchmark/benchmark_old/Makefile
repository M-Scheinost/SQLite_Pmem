CFLAGS= $(shell pkg-config --cflags libpmem) -Wall -I. ../sqlite /usr/local/include -O2 -DNDEBUG -std=c11 -lpmem
SRCS=$(wildcard *.c)
OBJS=$(SRCS:.c=.o)
HDRS=$(wildcard *.h)
TARGET=sqlite-bench 
LDFLAGS = $(shell pkg-config --libs libpmem) -O2 -lpthread -ldl -lm -static

$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $(TARGET) $(LDFLAGS)

%.o : %.c 
	$(CC) $(CFLAGS) -c $<

$(OBJS): $(HDRS)

bench: $(TARGET) clean-db
	./$(TARGET)

clean:
	rm -f $(TARGET) *.o

clean-db:
	rm -f dbbench_sqlite3*

.PHONY: bench clean clean-db

test:
	gcc -o bench -I. bench.h histogram.c main.c pmem_vfs.c benchmark.c random.c raw.c sqlite3.c util.c -O2 -L/usr/local/lib -ldl -lm -lpthread -lpmem

test_debug:
	gcc -o bench_debug -I. bench.h histogram.c main.c pmem_vfs.c benchmark.c random.c raw.c sqlite3.c util.c -O2 -L/usr/local/lib -ldl -lm -lpthread -lpmem