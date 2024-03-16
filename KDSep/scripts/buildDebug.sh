#!/bin/bash
cpuMaxThreadNumber=$(cat /proc/cpuinfo | grep processor | wc -l)
echo -e "\033[1m\033[32mDetect $cpuMaxThreadNumber cpu core on this machine, use all cores for compile\033[0m"

if [[ ! -d "/opt/RocksDB" || ! -f "/opt/RocksDB/librocksdb.a" ]]; then
    echo "Not found RocksDB in /opt"
    if [ ! -f "../RocksDB/librocksdb.a" ]; then
        echo "Not found librocksdb.a in ../RocksDB start build"
        cd ../RocksDB || exit
        make clean # clean up current build result since it may have errors
        make static_lib EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC USE_RTTI=1 DEBUG_LEVEL=0 -j$cpuMaxThreadNumber
        cd ../KDSep || exit
    fi
    echo "Copy rocksdb static lib to /opt"
    sudo mkdir -p /opt/RocksDB
    sudo cp -r ../RocksDB/include /opt/RocksDB
    sudo cp ../RocksDB/librocksdb.a /opt/RocksDB
else
    librocksdb="../RocksDB/librocksdb.a"
    md5value=$(md5sum $librocksdb)
    cd ../RocksDB/
    make static_lib EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC USE_RTTI=1 DEBUG_LEVEL=0 -j$cpuMaxThreadNumber
    cd -
    md5value2=$(md5sum $librocksdb)
    if [[ "$md5value" != "$md5value2" ]]; then
        echo "Copy rocksdb static lib to /opt"
        sudo mkdir -p /opt/RocksDB
        sudo cp -r ../RocksDB/include /opt/RocksDB
        sudo cp ../RocksDB/librocksdb.a /opt/RocksDB
    fi
fi

#./scripts/cleanup.sh

cd ./build || exit
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j$cpuMaxThreadNumber

cd .. || exit

exit

if [ ! -f "bin/test" ]; then
    echo -e "\033[31mBuild error, exit without testing \033[0m"
else
    echo -e "\n"
    ulimit -n 65536
    echo "Local Test with simple operations (Round 1) ===>"
    bin/test 1
    echo "Local Test with simple operations (Round 1) <==="
    echo "Local Test with simple operations (Round 2) ===>"
    bin/test 1
    echo "Local Test with simple operations (Round 2) <==="
fi
