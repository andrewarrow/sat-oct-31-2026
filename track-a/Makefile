CFLAGS ?= -O3 -std=c11 -Wall -Wextra -Wpedantic

solution: ingest.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f solution
