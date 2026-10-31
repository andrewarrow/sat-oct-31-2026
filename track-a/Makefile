CFLAGS ?= -O3 -march=native -std=c11 -Wall -Wextra -Wpedantic -pthread

solution: ingest.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f solution
