#!/bin/bash

func() {
    for runMode in "${runModeSet[@]}"; do
        opnum=`echo $op | sed 's/M/000000/g' | sed 's/K/000/g'`
        for readRatio in "${readRatios[@]}"; do
            ratio="0.$readRatio"
            if [[ $readRatio -eq 10 ]]; then
                ratio="1"
            fi

            if [[ "$runMode" == "raw" ]]; then
                scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
                    cache$cacheSize \
                    readRatio$ratio Exp$ExpName batchSize${batchSize} zipf${zipf} ${bonus} \
                    ${bonus2} ${bonus5} ${bonus4} \
                    finalScan${finalScan}
            elif [[ "$runMode" == "bkv" ]]; then
                scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
                    cache$cacheSize \
                    readRatio$ratio Exp$ExpName batchSize${batchSize} zipf${zipf} ${bonus} \
                    ${bonus2} ${bonus5} ${bonus4} finalScan${finalScan}
            elif [[ "$runMode" == "bkvkd" ]]; then
                if [[ "$ratio" == "1" ]]; then
                    bucketNumber=1024 
                fi
#               blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
                    cache$cacheSize kdcache${kdcacheSize} \
                    workerT$works bn$bucketNumber splitThres${splitThres} \
                    readRatio$ratio Exp$ExpName batchSize${batchSize} zipf${zipf} ${bonus} \
                    ${bonus2} ${bonus5} ${bonus4} finalScan${finalScan}
            elif [[ "$runMode" == "kv" ]]; then
                scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
                    cache$cacheSize \
                    readRatio$ratio Exp$ExpName batchSize${batchSize} zipf${zipf} ${bonus} \
                    ${bonus2} ${bonus5} ${bonus4} finalScan${finalScan}
            elif [[ "$runMode" == "kvkd" ]]; then
                if [[ "$ratio" == "1" ]]; then
                    bucketNumber=1024 
                fi
                blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
                    cache$cacheSize kdcache${kdcacheSize} \
                    workerT$works bn$bucketNumber batchSize${batchSize} splitThres${splitThres} \
                    readRatio$ratio Exp$ExpName zipf${zipf} ${bonus} \
                    ${bonus2} ${bonus5} ${bonus4} finalScan${finalScan}
            elif [[ "$runMode" == "kd" ]]; then
                if [[ "$ratio" == "1" ]]; then
                    bucketNumber=1024 
                fi
                blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
                    cache$cacheSize kdcache${kdcacheSize} \
                    workerT$works bn$bucketNumber splitThres${splitThres} \
                    readRatio$ratio Exp$ExpName batchSize${batchSize} zipf${zipf} ${bonus} \
                    ${bonus2} ${bonus5} ${bonus4} finalScan${finalScan}
            fi
        done
    done
}

works=8
rounds=1
batchSize=2
cacheSize=4096
splitThres=0.8
bucketNumber=32768
kdcacheSize=512
finalScan=0

### workload default settings
readRatios=(1)      # 10% reads
runModeSet=('raw')  # RocksDB
zipf="0.99"
fcl=10
flength=100
req="105M"
op="200M"

### Enable crash consistency 
bonus="ec" 

### Initialize
bonus2=""
bonus4=""
bonus5=""
