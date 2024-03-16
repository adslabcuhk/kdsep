CC=clang++
SEARCH_LIBRARY_ROCKSDB=/opt/RocksDB/include/
SEARCH_LIBRARY_KDSep=$(shell pwd)/../../KDSep/include/
# SEARCH_LIBRARY_LEVELDB=$(shell pwd)/../Databases/LevelDB/include/
LD_LIBRARY_ROCKSDB=/opt/RocksDB/librocksdb.a 
LD_LIBRARY_KDSep=$(shell pwd)/../../KDSep/lib/libKDSep.a
# LD_LIBRARY_LEVELDB=$(shell pwd)/../Databases/LevelDB/libleveldb.a
INCLUDES= -I ./ -I $(SEARCH_LIBRARY_ROCKSDB) -I ${SEARCH_LIBRARY_KDSep} #-I $(SEARCH_LIBRARY_LEVELDB)
#CFLAGS=-O3 -std=c++17 -g -Wall -pthread -ggdb -fsanitize=address
CFLAGS=-O3 -std=c++17 -Wall -pthread -ggdb # -fsanitize=address
LDFLAGS=$(LD_LIBRARY_ROCKSDB) $(LD_LIBRARY_KDSep) -lzstd -lpthread -llz4 -lbz2 -lsnappy -lz -ldl -luring -lboost_thread -lboost_system -lboost_serialization #$(LD_LIBRARY_LEVELDB) 
SUBDIRS=core db KDSep #RocksDB #LevelDB 
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc) $(wildcard KDSep/*.cc) # $(wildcard RocksDB/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)
.PHONY: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
		done
	$(RM) $(EXEC)
	rm -rf ./loadedDB
	rm -rf *.log



