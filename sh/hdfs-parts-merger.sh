#!/usr/bin/env bash
#USAGE: sh hdfs-parts-merger.sh [hdfs-location] [header]

hdfsLocation=$1
header=$2
dbTableName=`echo ${hdfsLocation} | cut -d'/' -f5`
dataFileName=`echo ${hdfsLocation} | cut -d'/' -f5 | cut -d'.' -f2`
dirName=/home/hadoop/hyuk0628/health-care-service/extracted_dataset

if ! test -d ${dirName}  ; then
    mkdir -p ${dirName}
fi

# Extract Data Set with merging parts files
hdfs dfs -getmerge ${hdfsLocation}/* ${dirName}/${dataFileName}.csv

# Extract the Data Set Header
# ssh -lhadoop nn1 "hive -e \"set hive.cli.print.header=false; show columns in ${dbTableName};\"" > ${dirName}/header

# Insert header to the Data Set
# sed -i ':a;N;$!ba;s/\n/,/g' ${dirName}/header # replace newline character to comma(,)
# header=`cat ${dirName}/header | sed -r 's/\s+//g'`
echo ${header}
sed -i -e '1i'${header}'\' ${dirName}/${dataFileName}.csv

# Remove header temp file
rm -rf ${dirName}/header