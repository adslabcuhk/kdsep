#CC=icpx
CC=clang++
CFLAGS=-O3 -std=c++17 -c -g
INCLUDES=-I ./ -I ../ -I $(shell pwd)/../../../KDSep/include/ -I $(shell pwd)/../../../RocksDB/include/
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

