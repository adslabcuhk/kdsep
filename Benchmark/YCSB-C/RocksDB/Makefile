CC=clang++ # icpx
CFLAGS=-O3 -std=c++17 -c -g
INCLUDES=-I ../ -I $(shell pwd)/../../../RocksDB/include/ -I ./
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

