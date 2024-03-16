# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=NaN
operationcount=NaN
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=0
readmodifywriteproportion=0
overwriteproportion=0
scanproportion=0
insertproportion=0


requestdistribution=zipfian
fieldcount=NaN
fieldlength=NaN
field_len_dist=constant
maxscanlength=100
zipfianconstant=0.9
