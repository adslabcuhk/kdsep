#/
find_package(Boost REQUIRED COMPONENTS thread system serialization)
if(Boost_FOUND)
   include_directories(${BOOST_INCLUDE_DIRS})
   link_directories(${BOOST_LIBRARY_DIR})
endif()

set(ROCKSDB_INCLUDE_DIR /opt/RocksDB/include)
set(ROCKSDB_STATIC_LIBRARY_OBJ /opt/RocksDB/librocksdb.a)

include_directories(../include ${ROCKSDB_INCLUDE_DIR})

set(BOOST_LIBRARY_OBJ boost_thread boost_system boost_filesystem boost_serialization)
set(SYSTEM_LIBRARY_OBJ pthread rt dl)
set(ROCKSDB_LIBRARY_OBJ lz4 z snappy bz2 zstd uring)

set(LINK_OBJ ${ROCKSDB_LIBRARY_OBJ} ${BOOST_LIBRARY_OBJ} ${SYSTEM_LIBRARY_OBJ})

set (CMAKE_SYSTEM_NAME Linux )
if (NOT CMAKE_BUILD_TYPE)
   set (CMAKE_BUILD_TYPE "Release")
   message (
      STATUS "No CMAKE_BUILD_TYPE selected, defaulting to ${CMAKE_BUILD_TYPE}"
   )
endif ()

if(${CMAKE_BUILD_TYPE} STREQUAL "Release")
   message (STATUS "CMAKE_BUILD_TYPE as Release selected")
   add_compile_options(-std=c++17 -fPIC -frtti -DXXH_PRIVATE_API) 
else()
   message (STATUS "CMAKE_BUILD_TYPE as Debug selected")
   add_compile_options(-Wall -std=c++17 -fPIC -frtti -g -ggdb -g2 -fno-omit-frame-pointer -fsanitize=leak -DXXH_PRIVATE_API) 
endif ()

add_library(KDSep STATIC 
      utils/bucketIndexBlock.cpp
      utils/bucketKeyFilter.cc
      utils/debug.cpp
      utils/headers.cc
      utils/fileOperation.cpp
      utils/mempool.cpp
      utils/murmurHash.cpp
      utils/statsRecorder.cc
      utils/timer.cpp
      utils/utils.cc
      utils/KDLRUCache.cpp
      utils/prefixTreeForHashStore.cpp
      deltaStore/hashStoreInterface.cpp
      deltaStore/bucketManager.cpp
      deltaStore/bucketOperator.cpp
      deltaStore/manifestManager.cpp
      vlog/deviceManager.cc
      vlog/ds/bitmap.cc
      vlog/ds/segmentPool.cc
      vlog/ds/lru.cc
      vlog/gcManager.cc
      vlog/indexStoreInterface.cpp
      vlog/indexStore.cpp
      vlog/configManager.cc
      vlog/indexStoreInterface.cpp
      vlog/logManager.cc
      vlog/rocksdbKeyManager.cc
      vlog/segmentGroupManager.cc
      vlog/valueManager.cc
      interface/KDSepOptions.cpp
      interface/KDSepInterface.cpp
      interface/lsmTreeInterface.cpp
      interface/mergeOperation.cpp
)

target_link_libraries(KDSep ${LINK_OBJ} ${ROCKSDB_STATIC_LIBRARY_OBJ})
# add_executable(deltaDump deltaStoreDump.cc)
# target_link_libraries(deltaDump KDSep ${LINK_OBJ} ${ROCKSDB_STATIC_LIBRARY_OBJ})
# add_executable(test mainTest.cc)
# target_link_libraries(test KDSep ${LINK_OBJ} ${ROCKSDB_STATIC_LIBRARY_OBJ})
