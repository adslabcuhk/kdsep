# Enhancing LSM-tree Key-Value Stores for Read-Modify-Writes via Key-Delta Separation

**This is the code repository for the submission of ICDE'24 paper (ID: 788): "Enhancing LSM-tree Key-Value Stores for Read-Modify-Writes via Key-Delta Separation".**

## Introduction

Read-modify-writes (RMWs) are increasingly observed in practical key-value (KV) storage workloads to support ﬁne-grained updates. To make RMWs efﬁcient, one approach is to write deltas (i.e., changes to current values) to the logstructured-merged-tree (LSM-tree), yet it also increases the read overhead caused by retrieving and combining a chain of deltas. We propose a notion called key-delta (KD) separation to support efﬁcient reads and RMWs in LSM-tree KV stores under RMWintensive workloads. The core idea of KD separation is to store deltas in separate storage and group deltas into storage units called buckets, such that all deltas of a key are kept in the same bucket and can be all together accessed in subsequent reads. To this end, we build KDSep, a middleware layer that realizes KD separation and integrates KDSep into state-of-the-art LSM-tree KV stores (e.g., RocksDB and BlobDB). We show that KDSep achieves signiﬁcant I/O throughput gains and read latency reduction under RMW-intensive workloads while preserving the efﬁciency in general workloads.

## Components

This repo mainly includes the following three components:

1. **RocksDB**: The underlying KV store acts as the LSM-tree in our design.
    * We use the [RocksDB 7.7.3](https://github.com/facebook/rocksdb/releases/tag/v7.7.3) version and added more breakdown information for performance analysis. It is stored in the `./RocksDB` directory.
2. **KDSep**: The middleware layer that realizes KD separation.
    * The source code of KDSep is stored in the `./KDSep` directory.
    * It integrates KDSep into the three baselines provided in the paper (RocksDB, BlobDB, and vLogDB).
3. **Benchmark**: The benchmark tool that is used to evaluate the performance of KDSep.
    * We use [YCSB-C](https://github.com/basicthinker/YCSB-C) as the benchmark tool, which is the `C++` version of [YCSB](https://github.com/brianfrankcooper/YCSB). The source code of YCSB-C is stored in the `./Benchmark/YCSB-C` directory.
    * We add support to RocksDB+KDSep, BlobDB+KDSep, and vLogDB+KDSep and corresponding baselines in YCSB-C.

## Dependencies

KDSep is tested on a single machine running Ubuntu 22.04 LTS with Linux kernel 5.15. The machine is equipped with a 16-core Intel Xeon Silver 4215 CPU, 96 GiB DDR4 memory, and a 3.84 TiB Western Digital Ultrastar DC SN640 NVMe SSD.

KDSep depends on the following packages that need to be installed manually or by package management tools.

1. **cmake**: CMake automated build framework.
2. **clang** and **llvm**: For compiling KDSep.
3. **libboost-all-dev**: For KDSep multithreading, message transmission, etc.
4. **libzstd-dev**, **liblz4-dev**, and **libsnappy-dev**: For underlying RocksDB compilation to provide services such as data compression.
5. **liburing-dev**: For the compilation of the benchmark tool `YCSB-C`. It provides helpers to set up and teardown io_uring instances in the Linux kernel.

To install `cmake`, `clang`, `llvm`, `libboost-all-dev`, `libzstd-dev`, `liblz4-dev`, `libsnappy-dev`, and `liburing-dev` via `apt` package management tool (on Ubuntu system), run the following command:

```shell
$ sudo apt-get install libboost-all-dev clang llvm libzstd-dev liblz4-dev libsnappy-dev liburing-dev
```

Note that if you need to install `liburing-dev` through the package management tool, the Ubuntu 22.04 LTS operating system is mandatory.


## Build

Since KDSep uses RocksDB as the underlying LSM-tree, we describe the compilation process in the order of compiling RocksDB, KDSep, and finally the benchmark.

### Build RocksDB

Compile the RocksDB as a static library as follows.

```shell
cd RocksDB
make static_lib EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC USE_RTTI=1 DEBUG_LEVEL=0
cd .. || exit
```

In addition, to further compile the KDSep and Benchmark, you need to put the compiled RocksDB library into the `/opt/RocksDB` directory as follows.

```shell
cd RocksDB
sudo mkdir -p /opt/RocksDB
sudo cp -r ../RocksDB/include /opt/RocksDB
sudo cp ../RocksDB/librocksdb.a /opt/RocksDB
cd .. || exit
```

### Build KDSep

Compile the KDSep library as follows.

```shell
cd KDSep
mkdir -p build
cd ./build || exit
cmake .. -DCMAKE_BUILD_TYPE=Release
make
cd .. || exit
```

### Build Benchmark

Compile the YCSB-C benchmark tools as follows.

```shell
cd Benchmark/YCSB-C
make
cd ../.. || exit
```

### Single-Step Build

Alternatively, we provide a script for the quick build, and you can use it as follows.

```shell
./buildAllComponents.sh
```

## Configuration

KDSep encapsulates the options of RocksDB and provides more specific options for setting the delta store. The following is an example to illustrate how to configure KDSep via the `KDSepOptions` object.

```cpp
// Create the KDSep options object.
KDSEP_NAMESPACE::KDSepOptions options_; 
// Set up underlying RocksDB by `options_.rocks_opt`, which is the encapsulated original rocksdb options.
options_.rocks_opt;
options_.enable_valueStore = true; // Enable the vLog as the KV separation scheme.
options_.enable_deltaStore = true; // Enable the delta store for key-delta separation in KDSep.
// The options for delta store
options_.enable_crash_consistency = true; // enable or disable the crash consistency mechanism (commit log and manifest file in the paper).
options_.enable_deltaStore_garbage_collection = true; // enable or disable the garbage collection mechanism in the delta store.
options_.deltaStore_gc_split_threshold_ = 0.8; // The split threshold of the delta store for garbage collection.
options_.deltaStore_max_bucket_number_ = 32768; // The maximum number of buckets in the delta store.
options_.deltaStore_bucket_size_ = 256 * 1024; // The size of each bucket in the delta store.
options_.deltaStore_init_k_ = 10; // The initial prefix tree height k in the delta store.
options_.deltaStore_KDCache_item_number_ = 1000000; // The KD cache size in the delta store (we use item_number * KD size as the KD cache size in use).
```

In addition, KDSep can also be configured based on `ini` file in the YCSB-C benchmark tool. You can change its configuration without rebuilding. The configuration file is located in `Benchmark/YCSB-C/confgDir/KDSep.ini`. In the configuration file, we describe the relevant settings in detail as the comments for each option.

## Usage

### Use KDSep as the KV Store

We have implemented the KDSep API with reference to RocksDB, and users can create a database that supports KDSep according to the following steps:

1. Include the KDSep header files in the C/C++ program.
```cpp 
#include "interface/KDSepInterface.hpp" // For the user API
#include "interface/KDSepOptions.hpp" // For the KDSep options
#include "interface/mergeOperation.hpp" // For the user-specific merge operations
```
2. Set up the KDSep options.
```cpp 
KDSEP_NAMESPACE::KDSepOptions options_; // Create a new KDSep options object.
... // Set up the KDSep options as the previous section.
```
3. Implement the user-defined merge operation.
```cpp
class KDSepFieldUpdateMergeOperator : public KDSepMergeOperator  {
   public:
   /**
    * Merge the deltas and value with the same key into the new value.
    * @param rawValue:    (IN) The value that's currently stored for this key.
    * @param operandList: (IN) the sequence of deltas to merge, front() first.
    * @param finalValue: (OUT) The merged new value for return.
    * @return                  Return true on success, false on failure/corruption/etc.
    */
    bool Merge(const str_t& rawValue, const vector<str_t>& operandList, string* finalValue) {
        // .... implement the merge operation here ....
        return true;
    }

   /**
    * This function performs merge(left_op, right_op) when both the operands are themselves merge operation types.
    * @param operands:         (IN) The original deltas need to perform the partial merge.
    * @param finalOperandList: (IN) the merged deltas for return. Typically, only one delta will be returned.
    * @return                  Return true on success, false on failure/corruption/etc.
    */
    bool PartialMerge(const vector<string>& operands, vector<string>& finalOperandList) {
        // .... implement the partial merge operation here ....
        return true;
    }
};
```
4. Create the KDSep database.
```cpp 
KDSEP_NAMESPACE::KDSep db_;
bool dbOpenStatus = db_.Open(options_, "Path to the DB storage"); // Use the KDSep options(1st parameter) and the storage position(2nd parameter) to create the KDSep database.
```
5. Use the operation APIs. KDSep provides `Put`, `Get`, `Merge`, and `Scan` interfaces as in RocksDB.
```cpp 
bool status;
/**
 * Put a KV pair into the database.
 * @param key: string, the key of the KV pair.
 * @param value: string, the value of the KV pair.
 * @return Bool value to indicate whether the operation is a success or not.
 */
status = db_.Put(key, value);
/**
 * Put a KD pair into the database.
 * @param key: string, the key of the KD pair.
 * @param delta: string, the delta of the KD pair.
 * @return Bool value to indicate whether the operation is a success or not.
 */
status = db_.Merge(key, delta);
/**
 * Get a KV pair from the database.
 * @param key: string, the key of the KV pair.
 * @param value: string, the retrieved value of the KV pair.
 * @return Bool value to indicate whether the operation is a success or not.
 */
status = db_.Get(key, &value);
/**
 * Read the specified number of KV pairs from the database.
 * @param key: string, the scan start key.
 * @param number: int, the number of KV pairs want to read.
 * @param keys: vector<string>, the list of retrieved keys of each KV pair.
 * @param values: vector<string>, the list of retrieved values of each KV pair.
 * @return Bool value to indicate whether the operation is a success or not.
 */
status = db_.Scan(key, number, keys, values);
```

### Benchmark

You can run the YCSB-C test program as follow, we use the testing of core workload as an example.

```shell
cd Benchmark/YCSB-C
./ycsbc -phase load -db KDSep -dbfilename loadedDB -threads 1 -P workloads/workloada.spec -configpath configDir/KDSep.ini
./ycsbc -phase run -db KDSep -dbfilename loadedDB -threads 1 -P workloads/workloada.spec -configpath configDir/KDSep.ini
```

Here, `-phase` is specific to the phase of the test, `-db` is specific to the kind of KV store(we only support `KDSep` here), `-dbfilename` is specific to the database storage path, `-threads` specific the number of threads to issue the operations to the database, `-P` specific the workload configuration file, and `-configpath` specific the configuration file of KDSep. After the test, YCSB-C will output an analysis (to the console) for each specified operation in the `workload.spec`.

## Reproduce Our Evaluations

We provide our evaluation script `Benchmark/YCSB-C/scripts/test.sh` to reproduce our evaluations. The script will automatically modify the parameter settings of YCSB-C and the corresponding KDSep settings. And save the results in logs named after the experiment name.

You can run the experiments as follows:

```shell
cd Benchmark/YCSB-C
$ scripts/exp.sh
```

These logs include the performance results of YCSB-C. The example of the logs is as follows:

```text
# Running operations:	100000000
# Transaction throughput (KTPS)
rocksdb	workload-temp.spec	1	26.6144	
run time: 3.75736e+09us

Read ops： 9997694
	Total read time: 3388.3 s
	Time per read: 0.338908 ms
Insert ops: 0
	Total insert time: 0 s
	Time per insert: -nan ms
Scan ops: 0
	Total scan time: 0 s
	Time per scan: -nan ms
Update ops: 90002306
	Total update time: 345.809 s
	Time per update: 0.00384223 ms
OverWrite ops: 0
	Total OverWrite time: 0 s
	Time per OverWrite: -nan ms
Read-Modify-Write ops: 0
	Total R-M-W time: 0 s
	Time per R-M-W: -nan ms
```
