#!/bin/bash

usage() {
    echo "Usage: $0 [kv] [kd] [bkv] [bs1000] [req1m]"
    echo "       kv: use KV separation (vLog)"
    echo "       kd: use KD separation (Delta store)"
    echo "      bkv: use BlobDB"
    echo "      raw: use RocksDB"
    echo "     kvkd: use KDSep"
    echo "   bs1000: Bucket size 1000"
    echo "    req1m: Totally 1M KV pairs"
    echo "     load: Load the database again"
    echo "     copy: Copy the database and do not run ycsbc"
}

generate_file_name() {
    i=1
    file=$1

    while [[ $i -lt 100 ]]; do
        filename="${file}-Round-${i}"
        if [[ -f "$filename.log" || -f "$filename" ]]; then
            i=$(($i + 1))
            continue
        fi
        break
    done

    echo "$filename.log"
}

config_workload() {
    SPEC=$1
    sed -i "/recordcount/c\\recordcount=$KVPairsNumber" $SPEC
    sed -i "/operationcount/c\\operationcount=$OperationsNumber" $SPEC
    sed -i "/fieldcount/c\\fieldcount=$fieldcount" $SPEC
    sed -i "/fieldlength/c\\fieldlength=$fieldlength" $SPEC
    if [[ $paretokey == "true" ]]; then
        sed -i "/field_len_dist/c\\field_len_dist=paretokey" $SPEC
    elif [[ $up2x == true ]]; then
        sed -i "/field_len_dist/c\\field_len_dist=up2x" $SPEC
        ReadProportion=0.0746
        UpdateProportion=0.9253
        sed -i "/overwriteproportion/c\\overwriteproportion=0.0001" $SPEC
    fi

    if [[ "$workloada" == "true" || "$workloadf" == "true" ]]; then
        ReadProportion=0.5
        UpdateProportion=0.5
        rmw="true"
    elif [[ "$workloadb" == "true" ]]; then
        ReadProportion=0.95
        UpdateProportion=0.05
        rmw="true"
    elif [[ "$workloadc" == "true" ]]; then
        ReadProportion=1
        UpdateProportion=0
        rmw="true"
    elif [[ "$workloadd" == "true" ]]; then
        sed -i "/readproportion/c\\readproportion=0.95" $SPEC
        sed -i "/insertproportion/c\\insertproportion=0.05" $SPEC
        sed -i "/requestdistribution/c\\requestdistribution=latest" $SPEC
        ReadProportion=0.95
        UpdateProportion=0
        rmw="true"
    elif [[ "$workloade" == "true" ]]; then
        sed -i "/scanproportion/c\\scanproportion=0.95" $SPEC
        sed -i "/insertproportion/c\\insertproportion=0.05" $SPEC
        ReadProportion=0
        UpdateProportion=0
    elif [[ "$workloadg" == "true" ]]; then
        sed -i "/scanproportion/c\\scanproportion=1" $SPEC
        ReadProportion=0
        UpdateProportion=0
    elif [[ "$workload2" == "true" ]]; then
        ReadProportion=0
        UpdateProportion=1
        rmw="false"
    elif [[ "$workload3" == "true" ]]; then
        sed -i "/overwriteproportion/c\\overwriteproportion=0.5" $SPEC
        ReadProportion=0
        UpdateProportion=0.5
        rmw="false"
    elif [[ "$workload4" == "true" ]]; then
        sed -i "/overwriteproportion/c\\overwriteproportion=1" $SPEC
        ReadProportion=0
        UpdateProportion=0
    fi

    if [[ "$ReadProportion" == "" ]]; then
        ReadProportion=0
    fi
    if [[ "$UpdateProportion" == "" ]]; then
        UpdateProportion=0
    fi

    sed -i "/readproportion/c\\readproportion=$ReadProportion" $SPEC
    if [[ "$rmw" == "false" ]]; then
        sed -i "/updateproportion/c\\updateproportion=$UpdateProportion" $SPEC
    elif [[ "$rmw" == "true" ]]; then
        sed -i "/readmodifywriteproportion/c\\readmodifywriteproportion=$UpdateProportion" $SPEC
    elif [[ "$rmw" == "overwrite" ]]; then
        sed -i "/overwriteproportion/c\\overwriteproportion=$UpdateProportion" $SPEC
    fi
}

log_db_status() {
    DBPath=$1
    ResultLogFile=$2

    output_file=tmpappend
    rm -rf $output_file
    echo "-------- smallest deltas ---------" >>$output_file
    ls -lt $DBPath | grep "delta" | sort -n -k5 | head >>$output_file
    echo "-------- largest deltas ----------" >>$output_file
    ls -lt $DBPath | grep "delta" | sort -n -k5 | tail >>$output_file
    echo "-------- delta sizes and counts --" >>$output_file
    ls -lt $DBPath | grep "delta" | awk '{s[$5]++;} END {for (i in s) {print i " " s[i];}}' | sort -k1 -n >>$output_file
    ls -lt $DBPath | grep "delta" | awk '{s+=$5; t++;} END {print s / 1024 / 1024 " MiB delta, num = " t;}' >>$output_file
    ls -lt $DBPath | grep "sst" | awk '{s+=$5; t++;} END {print s / 1024 / 1024 " MiB sst, num = " t;}' >>$output_file
    ls -lt $DBPath | grep "blob" | awk '{s+=$5; t++;} END {print s / 1024 / 1024 " MiB blob, num = " t;}' >>$output_file
    ls -lt $DBPath | grep "c0" | awk '{s+=$5;} END {print s / 1024 / 1024 " MiB vLog";}' >>$output_file
    echo "---------- total size ------------" >>$output_file
    du -d1 $DBPath | tail -n 1 | awk '{print $1 / 1024 " MiB all";}' >>$output_file
    echo "----------------------------------" >>$output_file

    lines=$(wc -l $output_file | awk '{print $1;}')
    cat $output_file >>$ResultLogFile
    cat temp.ini >>$ResultLogFile

    # dump LOG
    echo "Do not dump LOG for now"
    #    cp $DBPath/LOG $ResultLogFile-LOG

    # dump OPTIONS
    OPTIONS=$(ls -lht $DBPath/OPTIONS-* | head -n 1 | awk '{print $NF;}')
    #    cat $OPTIONS >> $ResultLogFile-LOG

    #    if [[ `echo "$ResultLogFile" | grep "Load" | wc -l` -ne 0 ]]; then
    #	if [[ -f /mnt/lvm/cleanLOG.sh ]]; then
    #	    /mnt/lvm/cleanLOG.sh $ResultLogFile-LOG
    #	fi
    #    fi
}

ulimit -n 204800
ulimit -s 102400
echo $@

PIDC=$$
echo "PID = $PIDC"

# ReadRatioSet=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9)
ReadProportion=0.1
OverWriteRatio=0.0
bn=4000
KVPairsNumber=10000000    #"300000000"
OperationsNumber=10000000 #"300000000"
fieldlength=400
fieldcount=10
DB_Working_Path="/mnt/g/KDSep/working"
DB_Loaded_Path="/mnt/d/KDSepltakvload"
if [[ ! -d "/mnt/g" ]]; then
    DB_Working_Path="/mnt/lvm/KDSep/working"
    DB_Loaded_Path="/mnt/lvm/KDSep"
fi
if [[ ! -d "/mnt/lvm" && ! -d "/mnt/g" ]]; then
    DB_Working_Path="/mnt/sn640/KDSepanonymous/working"
    DB_Loaded_Path="/mnt/sn640/KDSepanonymous"
fi
ResultLogFolder="Exp2/ResultLogs"
DB_Name="loadedDB"
MAXRunTimes=1
Thread_number=1
RocksDBThreadNumber=16
rawConfigPath="configDir/KDSep.ini"
bucketSize="$((256 * 1024))"
cacheSize="$((1024 * 1024 * 1024))"
kvCacheSize=0
blobCacheSize=0
kdcache=0
workerThreadNumber=8
gcThreadNumber=2
ds_gc_thres=0.9
ds_split_thres=0.8
batchSize=2 # In MiB
batchSizeK=0
cacheIndexFilter="true"
paretokey="false"
scanThreads=16
noReadAhead="false"
nogc="false"
bloomBits=10
maxOpenFiles=1048576
gcWriteBackSize=100000
enableParallel="false"
enableIndexBlock="true"
enableCrashConsistency="false"
disableMerge="false"
to=0
wbread=0
blobgcforce=1.0
# usage

cp $rawConfigPath ./temp.ini

suffix=""
run_suffix=""
only_copy=""
only_load=""
no_store="false"

cleanFlag="false"
usekv="false"
usekd="false"
usekvkd="false"
usebkv="false"
usebkvkd="false"
keep="false"
fix_workload="false"
workloada="false"
workloadb="false"
workloadc="false"
workloadd="false"
workloade="false"
workloadf="false"
workloadg="false"
workload2="false"
workload3="false"
workload4="false"
gc="true"
shortprepare="false"
blockSize=65536
fake="false"
nodirect="false"
nodirectreads="false"
usepwrite="false"
nommap="false"
checkRepeat="false"
rmw="false"
up2x="false"
recovery="false"
crash="false"
crashTime=3600

flushSize=4092
sstsz=64
l1sz=256
memtable=64
memSize=""
initBit=20
unsort=1024

havekd="false"

for param in $*; do
    if [[ $param == "kv" ]]; then
        suffix=${suffix}_kv
        usekv="true"
        sed -i "/keyValueSeparation/c\\keyValueSeparation = true" temp.ini
    elif [[ $param == "kd" ]]; then
        suffix=${suffix}_kd
        usekd="true"
        havekd="true"
        sed -i "/keyDeltaSeparation/c\\keyDeltaSeparation = true" temp.ini
    elif [[ $param == "raw" ]]; then
        suffix=${suffix}_raw
    elif [[ $param == "kvkd" ]]; then
        suffix=${suffix}_kvkd
        usekvkd="true"
        havekd="true"
        sed -i "/keyValueSeparation/c\\keyValueSeparation = true" temp.ini
        sed -i "/keyDeltaSeparation/c\\keyDeltaSeparation = true" temp.ini
    elif [[ $param == "bkv" ]]; then
        suffix=${suffix}_bkv
        usebkv="true"
        sed -i "/blobDbKeyValueSeparation/c\\blobDbKeyValueSeparation = true" temp.ini
    elif [[ $param == "bkvkd" ]]; then
        suffix=${suffix}_bkvkd
        usebkvkd="true"
        havekd="true"
        sed -i "/keyDeltaSeparation/c\\keyDeltaSeparation = true" temp.ini
        sed -i "/blobDbKeyValueSeparation/c\\blobDbKeyValueSeparation = true" temp.ini
    elif [[ $param == "copy" ]]; then
        only_copy="true"
    elif [[ $param == "load" ]]; then
        only_load="true"
    elif [[ $param == "no_store" ]]; then
        no_store="true"
    elif [[ "$param" == "req" || "$param" == "op" || "$param" == "readRatio" ]]; then
        echo "Param error: $param"
        exit
    elif [[ "$param" =~ ^req[0-9]+[mMkK]*$ ]]; then # req10m
        num=$(echo $param | sed 's/req//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g')
        KVPairsNumber=$num
    elif [[ "$param" =~ ^op[0-9]+[mMkK]*$ ]]; then
        num=$(echo $param | sed 's/op//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g')
        #        if [[ $num -eq 20000000 ]]; then
        #            echo "Alert! temporary change"
        #            num=50000000
        #        fi
        OperationsNumber=$num
    elif [[ "$param" =~ ^fc[0-9]+$ ]]; then
        num=$(echo $param | sed 's/fc//g')
        fieldcount=$num
    elif [[ "$param" =~ ^fl[0-9]+$ ]]; then
        num=$(echo $param | sed 's/fl//g')
        fieldlength=$num
    elif [[ "$param" =~ ^readRatio[0-9].[0-9]*$ || "$param" == "readRatio1" ]]; then
        ReadProportion=$(echo $param | sed 's/readRatio//g')
        if [[ "$ReadProportion" == "0.9" ]]; then
            exit
        fi
    elif [[ "$param" =~ ^bn[0-9]+$ ]]; then
        bn=$(echo $param | sed 's/bn//g')
        run_suffix=${run_suffix}_${param}
    elif [[ "$param" =~ ^initBit[0-9]+$ ]]; then
        tmp=$(echo $param | sed 's/initBit//g')
        if [[ $tmp -ne $initBit && "$havekd" == "true" ]]; then
            initBit=$tmp
            run_suffix=${run_suffix}_${param}
        fi
    elif [[ "$param" =~ ^unsort[0-9]+$ ]]; then
        tmp=$(echo $param | sed 's/unsort//g')
        if [[ $tmp -ne $unsort && "$havekd" == "true" ]]; then
            unsort=$tmp
            run_suffix=${run_suffix}_${param}
        fi
    elif [[ "$param" =~ ^Exp[0-9a-zA-Z_]+$ ]]; then
        ExpID=$(echo $param | sed 's/Exp//g')
        ResultLogFolder="Exp$ExpID/ResultLogs"
        if [ ! -d $DB_Working_Path ]; then
            mkdir -p $DB_Working_Path
        fi
        if [ ! -d $DB_Loaded_Path ]; then
            mkdir -p $DB_Loaded_Path
        fi
    elif [[ "$param" =~ ^note ]]; then
        note=$(echo $param | sed 's/note//g')
        run_suffix=${run_suffix}_${note}
    elif [[ "$param" =~ ^threads[0-9]+$ ]]; then
        tmp=$(echo $param | sed 's/threads//g')
        if [[ $tmp -ne $RocksDBThreadNumber ]]; then
            RocksDBThreadNumber=$tmp
            run_suffix=${run_suffix}_thd${RocksDBThreadNumber}
        fi
    elif [[ "$param" =~ ^gcT[0-9]+$ ]]; then
        tmp=$(echo $param | sed 's/gcT//g')
        if [[ "$tmp" != "$gcThreadNumber" && "$havekd" == "true" ]]; then
            gcThreadNumber=$tmp
            run_suffix=${run_suffix}_${param}
        fi
    elif [[ "$param" =~ ^gcThres[0-9.]+$ ]]; then
        tmp=$(echo $param | sed 's/gcThres//g')
        if [[ "$tmp" != "$ds_gc_thres" ]]; then
            ds_gc_thres=$(echo $param | sed 's/gcThres//g')
            run_suffix=${run_suffix}_${param}
        fi
    elif [[ "$param" =~ ^scanThreads[0-9.]+$ ]]; then
        tmp=$(echo $param | sed 's/scanThreads//g')
        if [[ "$tmp" != "$scanThreads" ]]; then
            scanThreads=$tmp
            run_suffix=${run_suffix}_${param}
        fi
    elif [[ "$param" =~ ^noReadAhead$ ]]; then
        noReadAhead="true"
        run_suffix=${run_suffix}_noReadAhead
    elif [[ "$param" =~ ^splitThres[0-9.]+$ ]]; then
        tmp=$(echo $param | sed 's/splitThres//g')
        if [[ "$tmp" != "$ds_split_thres" ]]; then
            ds_split_thres=$(echo $param | sed 's/splitThres//g')
            run_suffix=${run_suffix}_sp${tmp}
        fi
    elif [[ "$param" =~ ^workerT[0-9]+$ ]]; then
        tmp=$(echo $param | sed 's/workerT//g')
        if [[ "$tmp" != "$workerThreadNumber" && "$havekd" == "true" ]]; then
            workerThreadNumber=$tmp
            run_suffix=${run_suffix}_${param}
        fi
    elif [[ "$param" =~ ^bucketSize[0-9]+$ ]]; then
        bucketSize=$(echo $param | sed 's/bucketSize//g')
        run_suffix=${run_suffix}_${param}
    elif [[ "$param" =~ ^batchSize[0-9]+$ ]]; then
        tmp=$(echo $param | sed 's/batchSize//g')
        if [[ "$tmp" != "$batchSize" ]]; then
            batchSize=$tmp
            run_suffix=${run_suffix}_bs${tmp}M
        fi
    elif [[ "$param" =~ ^batchSize[0-9]+K$ ]]; then
        tmp=$(echo $param | sed 's/batchSize//g' | sed 's/K//g')
        if [[ "$tmp" != "$batchSizeK" ]]; then
            batchSizeK=$tmp
            run_suffix=${run_suffix}_bs${tmp}K
        fi
    elif [[ "$param" =~ ^round[0-9]+$ ]]; then
        MAXRunTimes=$(echo $param | sed 's/round//g')
    elif [[ "$param" =~ ^timeout[0-9]+$ ]]; then
        tmp=$(echo $param | sed 's/timeout//g')
        if [[ $tmp -ne $to ]]; then
            run_suffix=${run_suffix}_to${tmp}
            to=$tmp
        fi
    elif [[ "$param" =~ ^cache[0-9]+$ ]]; then
        num=$(echo $param | sed 's/cache//g')
        cacheSize=$(($num * 1024 * 1024))
        run_suffix=${run_suffix}_bkc${num}
    elif [[ "$param" =~ ^kvcache[0-9]+$ ]]; then
        num=$(echo $param | sed 's/kvcache//g')
        kvCacheSize=$(($num * 1024 * 1024))
        run_suffix=${run_suffix}_kvc${num}
    elif [[ "$param" =~ ^blobcache[0-9]+$ ]]; then
        num=$(echo $param | sed 's/blobcache//g')
        blobCacheSize=$(($num * 1024 * 1024))
        run_suffix=${run_suffix}_blc${num}
    elif [[ "$param" =~ ^wbread[0-9]+$ ]]; then
        num=$(echo $param | sed 's/wbread//g')
        if [[ $wbread -ne $num && $havekd == "true" ]]; then
            wbread=$num
            run_suffix=${run_suffix}_wbread${num}
        fi
    elif [[ "$param" =~ ^blobgcforce[0-9.]+$ ]]; then
        num=$(echo $param | sed 's/blobgcforce//g')
        if [[ $num != "1.0" && ("$usebkv" == "true" || "$usebkvkd" == "true") ]]; then
            run_suffix=${run_suffix}_${param}
            blobgcforce=$num
        fi
    elif [[ "$param" =~ ^kdcache[0-9]+$ ]]; then
        num=$(echo $param | sed 's/kdcache//g')
        kdcache=$(($num * 1024 * 1024))
        run_suffix=${run_suffix}_kdc${num}
    elif [[ "$param" =~ ^blockSize[0-9]+$ ]]; then
        blockSize=$(echo $param | sed 's/blockSize//g')
        if [[ $blockSize -ne 65536 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^gcWriteBackSize[0-9]+$ ]]; then
        num=$(echo $param | sed 's/gcWriteBackSize//g')
        if [[ $gcWriteBackSize != $num ]]; then
            gcWriteBackSize=$num
            run_suffix=${run_suffix}_gcwbsz${gcWriteBackSize}
        fi
    elif [[ "$param" =~ ^flushSize[0-9]+$ ]]; then
        tmp=$(echo $param | sed 's/flushSize//g')
        if [[ $tmp -ne $flushSize && "$havekd" == "true" ]]; then
            flushSize=$tmp
            run_suffix=${run_suffix}_${param}
        fi
    elif [[ "$param" =~ ^sst[0-9]+$ ]]; then
        sstsz=$(echo $param | sed 's/sst//g')
        if [[ $sstsz -ne 64 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^l1sz[0-9]+$ ]]; then
        l1sz=$(echo $param | sed 's/l1sz//g')
        if [[ $l1sz -ne 256 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^memtable[0-9]+$ ]]; then
        memtable=$(echo $param | sed 's/memtable//g')
        if [[ $memtable -ne 64 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^mem[0-9gGmM]+$ ]]; then
        memSize=$(echo $param | sed 's/mem//g')
        run_suffix=${run_suffix}_$param
    elif [[ "$param" =~ ^bf[0-9]+$ ]]; then
        bloomBits=$(echo $param | sed 's/bf//g')
        if [[ $bloomBits -ne 10 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^open[0-9]+$ ]]; then
        tmp=$(echo $param | sed 's/open//g')
        if [[ $maxOpenFiles -ne $tmp ]]; then
            maxOpenFiles=$tmp
            run_suffix=${run_suffix}_$param
        fi
    elif [[ "$param" =~ ^zipf[0-9.]+$ ]]; then
        tmp=$(echo $param | sed 's/zipf//g')
        sed -i "/const double kZipfianConst/c\\    constexpr static const double kZipfianConst = ${tmp};" core/zipfian_generator.h
        scripts/make_release.sh
        if [[ $tmp != "0.9" ]]; then
            sed -i "/const double kZipfianConst/c\\    constexpr static const double kZipfianConst = 0.9;" core/zipfian_generator.h
            run_suffix=${run_suffix}_$param
            if (($(echo "$tmp >= 1.0" | bc -l))); then
                filename="zipf${tmp}_$(($KVPairsNumber / 1000000))M_$(($OperationsNumber / 1000000))M.data"
                echo $filename
                if [[ ! -f $filename ]]; then
                    Rscript scripts/gen.r $tmp $KVPairsNumber $OperationsNumber
                    cp out.data $filename
                fi
                cp $filename out.data
            fi
        fi
    elif [[ "$param" =~ ^clean$ ]]; then
        cleanFlag="true"
    elif [[ "$param" == "keep" ]]; then
        keep="true"
        run_suffix=${run_suffix}_keep
    elif [[ "$param" =~ ^workload[a-g2-9]$ ]]; then
        if [[ $fix_workload == "true" ]]; then
            echo "error: repeated workload"
            exit
        fi
        fix_workload="true"
        if [[ "$param" == "workloada" ]]; then
            workloada="true"
        elif [[ "$param" == "workloadb" ]]; then
            workloadb="true"
        elif [[ "$param" == "workloadc" ]]; then
            workloadc="true"
        elif [[ "$param" == "workloadd" ]]; then
            workloadd="true"
        elif [[ "$param" == "workloade" ]]; then
            workloade="true"
        elif [[ "$param" == "workloadf" ]]; then
            workloadf="true"
        elif [[ "$param" == "workloadg" ]]; then
            workloadg="true"
        elif [[ "$param" == "workload2" ]]; then
            workload2="true"
        elif [[ "$param" == "workload3" ]]; then
            workload3="true"
        elif [[ "$param" == "workload4" ]]; then
            workload4="true"
        fi
        run_suffix=${run_suffix}_$param
    elif [[ "$param" == "shortprepare" ]]; then
        shortprepare="true"
    elif [[ "$param" == "nocif" ]]; then
        cacheIndexFilter="false"
        run_suffix=${run_suffix}_nocif
    elif [[ "$param" == "fake" ]]; then
        fake="true"
        run_suffix=${run_suffix}_fake
    elif [[ "$param" == "ep" ]]; then
        if [[ "$havekd" == "true" ]]; then
            enableParallel="true"
            run_suffix=${run_suffix}_ep
        fi
    elif [[ "$param" == "di" ]]; then
        if [[ "$havekd" == "true" ]]; then
            enableIndexBlock="false"
            run_suffix=${run_suffix}_di
        fi
    elif [[ "$param" == "ec" ]]; then
        if [[ "$havekd" == "true" ]]; then
            enableCrashConsistency="true"
            run_suffix=${run_suffix}_ec
        fi
    elif [[ "$param" == "dm" ]]; then
        if [[ "$havekd" == "true" ]]; then
            disableMerge="true"
            run_suffix=${run_suffix}_dm
        fi
    elif [[ "$param" == "nodirect" ]]; then
        nodirect="true"
        run_suffix=${run_suffix}_nodirect
    elif [[ "$param" == "nodirectreads" ]]; then
        nodirectreads="true"
        run_suffix=${run_suffix}_nodirectreads
    elif [[ "$param" == "usepwrite" ]]; then
        usepwrite="true"
        run_suffix=${run_suffix}_usepwrite
    elif [[ "$param" == "nommap" ]]; then
        nommap="true"
        run_suffix=${run_suffix}_nommap
    elif [[ "$param" == "paretokey" ]]; then
        paretokey="true"
    elif [[ "$param" == "nogc" ]]; then
        nogc="true"
    elif [[ "$param" == "checkrepeat" ]]; then
        checkRepeat="true"
    elif [[ "$param" == "up2x" ]]; then
        up2x="true"
        fieldlength=48
    elif [[ "$param" == "rmw" ]]; then
        rmw="true"
    elif [[ "$param" == "overwrite" ]]; then
        rmw="overwrite"
    elif [[ "$param" == "recovery" ]]; then
        recovery="true"
        run_suffix=${run_suffix}_recovery
    elif [[ "$param" =~ ^crash[0-9]+$ ]]; then
        crash="true"
        crashTime=$(echo $param | sed 's/crash//g')
        rTime=$RANDOM
        crashTime=$(($rTime % $crashTime + 60))
        run_suffix=${run_suffix}_${param}_${crashTime}
        echo "${param} ${crashTime}"
    fi
done

# KV cache

if [[ $kvCacheSize -ne 0 ]]; then
    if [[ "$usekd" == "false" && "$usebkvkd" == "false" && "$usekvkd" == "false" ]]; then
        bn=0
    fi
    KDSepCacheSize=$((($kvCacheSize - $bn * 4096) / ($fieldcount * $fieldlength + $fieldcount - 1)))
    sed -i "/enableKDSepCache/c\\enableKDSepCache = true" temp.ini
    sed -i "/KDSepCacheObjectNumber/c\\KDSepCacheObjectNumber = $KDSepCacheSize" temp.ini
else
    sed -i "/enableKDSepCache/c\\enableKDSepCache = false" temp.ini
fi

if [[ "$usekd" == "true" || "$usebkvkd" == "true" || "$usekvkd" == "true" ]]; then
    sed -i "/ds_init_bit/c\\ds_init_bit = $initBit" temp.ini
    sed -i "/ds_bucket_buffer_size/c\\ds_bucket_buffer_size = $flushSize" temp.ini
    sed -i "/ds_bucket_num/c\\ds_bucket_num = $bn" temp.ini
    if [[ $kdcache -ne 0 ]]; then
        sed -i "/ds_kdcache_size/c\\ds_kdcache_size = $kdcache" temp.ini
    fi

    sed -i "/ds_worker_thread_number_limit/c\\ds_worker_thread_number_limit = $workerThreadNumber" temp.ini
    sed -i "/ds_gc_thread_number_limit/c\\ds_gc_thread_number_limit = $gcThreadNumber" temp.ini

    sed -i "/ds_gc_thres/c\\ds_gc_thres = $ds_gc_thres" temp.ini
    sed -i "/ds_split_thres/c\\ds_split_thres = $ds_split_thres" temp.ini
    sed -i "/ds_bucket_size/c\\ds_bucket_size = $bucketSize" temp.ini
    sed -i "/ds_gc_write_back_size/c\\ds_gc_write_back_size = $gcWriteBackSize" temp.ini
    sed -i "/unsorted_part_size_threshold/c\\unsorted_part_size_threshold = $((${unsort} * 1024))" temp.ini
    if [[ $wbread -ne 10 ]]; then
        sed -i "/ds_read_write_back_num/c\\ds_read_write_back_num = $wbread" temp.ini
        sed -i "/ds_read_write_back_size/c\\ds_read_write_back_size = $wbread" temp.ini
    fi
fi

sed -i "/write_buffer_size/c\\write_buffer_size = $(($batchSize * 1024 * 1024))" temp.ini
if [[ $batchSizeK != "0" ]]; then
    sed -i "/write_buffer_size/c\\write_buffer_size = $(($batchSizeK * 1024))" temp.ini
fi
sed -i "/blockCache/c\\blockCache = $cacheSize" temp.ini
sed -i "/blobCacheSize/c\\blobCacheSize = ${blobCacheSize}" temp.ini
sed -i "/numThreads/c\\numThreads = ${RocksDBThreadNumber}" temp.ini
sed -i "/blockSize/c\\blockSize = ${blockSize}" temp.ini
sed -i "/blobgcforce/c\\blobgcforce = ${blobgcforce}" temp.ini
totCacheSize=$(((${kvCacheSize} + $kdcache + $cacheSize + $blobCacheSize) / 1024 / 1024))
run_suffix=${run_suffix}_tc${totCacheSize}

if [[ "$cacheIndexFilter" == "true" ]]; then
    sed -i "/cacheIndexAndFilterBlocks/c\\cacheIndexAndFilterBlocks = true" temp.ini
fi

if [[ "$fake" == "true" ]]; then
    sed -i "/fakeDirectIO/c\\fakeDirectIO = true" temp.ini
fi

if [[ "$nodirect" == "true" ]]; then
    sed -i "/directIO/c\\directIO = false" temp.ini
fi

if [[ "$nodirectreads" == "true" ]]; then
    sed -i "/directReads/c\\directReads = false" temp.ini
fi

if [[ "$enableParallel" == "true" ]]; then
    sed -i "/parallel_lsm_tree_interface/c\\parallel_lsm_tree_interface = true" temp.ini
fi

if [[ "$enableIndexBlock" == "false" ]]; then
    sed -i "/enable_index_block/c\\enable_index_block = false" temp.ini
fi

if [[ "$enableCrashConsistency" == "true" ]]; then
    sed -i "/crash_consistency/c\\crash_consistency = true" temp.ini
fi

if [[ "$disableMerge" == "true" ]]; then
    sed -i "/enable_bucket_merge/c\\enable_bucket_merge = false" temp.ini
fi

if [[ "$nommap" == "true" ]]; then
    sed -i "/useMmap/c\\useMmap = false" temp.ini
fi

numMainSegment="$(($KVPairsNumber * (24 + $fieldcount * $fieldlength) / 10 * 15 / 1048576))"
if [[ $numMainSegment -le 100 ]]; then
    echo "test: numMainSegment 100"
    numMainSegment=100
fi
sed -i "/numMainSegment/c\\numMainSegment = $numMainSegment" temp.ini
sed -i "/numRangeScanThread/c\\numRangeScanThread = $scanThreads" temp.ini
if [[ $noReadAhead == "true" ]]; then
    sed -i "/enableScanReadAhead/c\\enableScanReadAhead = 0" temp.ini
fi

size="$(($KVPairsNumber / 1000000))M"
if [[ $size == "0M" ]]; then
    size="$(($KVPairsNumber / 1000))K"
elif [[ "$(($KVPairsNumber % 1000000))" -ne 0 ]]; then
    echo "$(($KVPairsNumber % 1000000))"
    size="${size}$((($KVPairsNumber % 1000000) / 1000))K"
fi

ops="op$(($OperationsNumber / 1000000))M"
if [[ $ops == "0M" ]]; then
    ops="op$(($OperationsNumber / 1000))K"
elif [[ "$(($OperationsNumber % 1000000))" -ne 0 ]]; then
    ops="${ops}$((($OperationsNumber % 1000000) / 1000))K"
fi

if [[ $paretokey == "true" ]]; then
    suffix=${suffix}_fc${fieldcount}_paretokey_${size}
elif [[ $up2x == "true" ]]; then
    suffix=${suffix}_fc${fieldcount}_up2x_${size}
else
    suffix=${suffix}_fc${fieldcount}_fl${fieldlength}_${size}
fi

if [[ $nogc == "true" ]]; then
    sed -i "/deltaStoreEnableGC/c\\deltaStoreEnableGC = false" temp.ini
fi

if [[ $recovery == "true" ]]; then
    sed -i "/test_recovery/c\\test_recovery = true" temp.ini
fi

max_kv_size=$((((${fieldcount} * (2+${fieldlength}) + 4) + 4095) / 4096 * 4096))
sed -i "/max_kv_size/c\\max_kv_size = $max_kv_size" temp.ini
sed -i "/memtable/c\\memtable = $(($memtable * 1024 * 1024))" temp.ini
sed -i "/sst_size/c\\sst_size = $(($sstsz * 1024 * 1024))" temp.ini
sed -i "/l1_size/c\\l1_size = $(($l1sz * 1024 * 1024))" temp.ini
sed -i "/bloomBits/c\\bloomBits = $bloomBits" temp.ini
sed -i "/maxOpenFiles/c\\maxOpenFiles = $maxOpenFiles" temp.ini

DB_Name=${DB_Name}${suffix}
ResultLogFolder=${ResultLogFolder}${suffix}
configPath="temp.ini"

if [ ! -d $ResultLogFolder ]; then
    mkdir -p $ResultLogFolder
fi

SPEC="workload-temp.spec"
if [ -f $SPEC ]; then
    rm -rf $SPEC
    echo "Deleted old workload spec"
fi

loaded="false"
workingDB=${DB_Working_Path}/workingDB
loadedDB=${DB_Loaded_Path}/${DB_Name}

if [[ "$memSize" != "" ]]; then
    set -x
    sudo cgset -r memory.max=${memSize} zz2
    sudo cgclassify -g memory:zz2 --sticky $PIDC
    set +x
fi

if [[ "$only_load" != "true" ]]; then
    if [[ "$usekd" == "true" ]]; then
        loadedDB="$(echo $loadedDB | sed "s/kd/raw/g")"
    elif [[ "$usebkvkd" == "true" ]]; then
        loadedDB="$(echo $loadedDB | sed "s/bkvkd/bkv/g")"
    elif [[ "$usekvkd" == "true" ]]; then
        loadedDB="$(echo $loadedDB | sed "s/kvkd/kv/g")"
    fi
    echo "Real loadedDB $loadedDB"
fi

if [[ ! -d $loadedDB ]]; then
    echo "no loaded db $loadedDB"
fi

echo "<===================== Loading the database =====================>"

if [[ "$cleanFlag" == "true" ]]; then
    set -x
    rm -rf $loadedDB
    rm -rf $workingDB
    exit
fi

if [[ ! -d $loadedDB || "$only_load" == "true" ]]; then
    echo "Modify spec for load"
    SPEC="./workload-temp.spec"
    cp workloads/workloadTemplate.spec $SPEC
    config_workload $SPEC

    rm -rf $loadedDB
    if [[ -d $workingDB ]]; then
        rm -rf $workingDB
    fi
    output_file=$(generate_file_name $ResultLogFolder/LoadDB${run_suffix})
    echo "output at $output_file"
    ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase load -configpath $configPath >${output_file}
    #    gdb --args ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase load -configpath $configPath # > ${output_file}
    retvalue=$?
    loaded="true"
    echo "output at $output_file"
    if [[ "$no_store" == "true" ]]; then
        echo "no store, exit"
        exit
    fi

    t_output_file=$output_file
    log_db_status $workingDB $t_output_file
    output_file=${t_output_file}

    if [[ $retvalue -ne 0 ]]; then
        echo "Exit. return number $retvalue"
        exit
    fi

    # Running Update
    echo "Read loaded DB to complete compaction"
    if [ -f workload-temp.spec ]; then
        rm -rf workload-temp.spec
        echo "Deleted old workload spec"
    fi

    SPEC="./workload-temp-prepare.spec"
    cp workloads/workloadTemplate.spec $SPEC
    config_workload $SPEC
    if [[ "$shortprepare" == "true" ]]; then
        sed -i "/operationcount/c\\operationcount=10000" $SPEC
    else
        sed -i "/operationcount/c\\operationcount=3000000" $SPEC
    fi
    sed -i "/readproportion/c\\readproportion=1" $SPEC
    sed -i "/updateproportion/c\\updateproportion=0" $SPEC
    echo "<===================== Prepare =====================>"
    ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P ${SPEC} -phase run -configpath $configPath >${output_file}-prepare
    echo "output at ${output_file}-prepare"
    rm -f $SPEC

    cp -r $workingDB $loadedDB # Copy loaded DB
    if [[ "$only_load" == "true" ]]; then
        exit
    fi
fi

run_suffix="${run_suffix}-${ops}"

for ((roundIndex = 1; roundIndex <= MAXRunTimes; roundIndex++)); do

    if [ -f workload-temp.spec ]; then
        rm -rf workload-temp.spec
        echo "Deleted old workload spec"
    fi
    UpdateProportion=$(echo "" | awk '{print 1.0-'"$ReadProportion"';}')
    echo "Modify spec, Read/Update ratio = $ReadProportion:$UpdateProportion"
    SPEC="./workload-temp.spec"
    cp workloads/workloadTemplate.spec $SPEC
    config_workload $SPEC
    echo "<===================== Modified spec file content =====================>"
    #    cat workload-temp.spec | head -n 25 | tail -n 17

    fileprefix=$ResultLogFolder/Rd-$ReadProportion-Ud-$UpdateProportion-${run_suffix}
    if [[ "$rmw" == "true" ]]; then
        fileprefix=$ResultLogFolder/Rd-$ReadProportion-RMW-$UpdateProportion-${run_suffix}
    elif [[ "$rmw" == "overwrite" ]]; then
        fileprefix=$ResultLogFolder/Rd-$ReadProportion-OW-$UpdateProportion-${run_suffix}
    fi

    output_file=$(generate_file_name ${fileprefix})

    echo "output at $output_file"
    if [[ "$checkRepeat" == "true" ]]; then
        if [[ $(echo $output_file | grep "Round-1" | wc -l) -eq 0 ]]; then
            echo "exit because of check repeated: $output_file"
            exit
        fi
    fi

    # Running the ycsb-benchmark
    if [[ "$loaded" == "false" ]]; then
        if [[ "$recovery" == "false" ]]; then
            if [ -d $workingDB ]; then
                rm -rf $workingDB
                echo "Deleted old database folder"
            fi
            echo "cp -r $loadedDB $workingDB"
            cp -r $loadedDB $workingDB
            echo "Copy loaded database"
        fi
    fi
    if [ ! -d $workingDB ]; then
        echo "Retrived loaded database error"
        exit
    fi
    if [[ $only_copy == "true" ]]; then
        exit
    fi

    echo "<===================== Benchmark the database (Round $roundIndex) start =====================>"

    set -x
    if [[ $to -ne 0 ]]; then
        timeout -k ${to}s ${to}s ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase run -configpath $configPath >$output_file &
        newpid=$!
        wait $newpid
    elif [[ "$crash" == "true" ]]; then
        ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase run -configpath $configPath >$output_file &
        newpid=$!
        echo "wait for $crashTime seconds"
        sleep $crashTime
        echo "kill $newpid"
        kill -9 $newpid
        wait $newpid
    else
        ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase run -configpath $configPath >$output_file
        #        gdb --args ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase run -configpath $configPath
        #        exit
    fi
    set +x
    loaded="false"
    echo "output at $output_file"
    log_db_status $workingDB $output_file
    echo "<===================== Benchmark the database (Round $roundIndex) done =====================>"
    # Cleanup
    if [ -f $SPEC ]; then
        rm -rf $SPEC
        echo "Deleted old workload spec"
    fi
    if [[ $roundIndex -eq $MAXRunTimes ]]; then
        if [ -f temp.ini ]; then
            rm -rf temp.ini
            echo "Deleted old workload config"
        fi
    fi
done
