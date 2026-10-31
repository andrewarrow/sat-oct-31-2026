CFLAGS ?= -O3 -march=native -std=c11 -Wall -Wextra -Wpedantic -pthread
WORKERS ?= 12

solution: ingest.c
	$(CC) $(CFLAGS) -DWORKERS=$(WORKERS) $(LDFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f solution
