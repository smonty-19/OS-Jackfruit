# Root Makefile - forwards build to boilerplate/
.PHONY: all clean ci

all:
	$(MAKE) -C boilerplate

ci:
	$(MAKE) -C boilerplate ci

clean:
	$(MAKE) -C boilerplate clean