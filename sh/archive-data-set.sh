#!/usr/bin/env bash
#USAGE: sh archive-data-set.sh [archiveFileName] [ftpLocation]

archiveFileName=$1
ftpLocation=$2

datasetDirName=/home/hadoop/hyuk0628/health-care-service/extracted_dataset
dirName=/home/hadoop/hyuk0628/health-care-service/archive

if ! test -d ${dirName} ; then
    mkdir -p ${dirName}
fi

find ${datasetDirName}/* -size 0 -exec rm -rf {} \;
cd ${datasetDirName}

if ! test -d ${dirName}/${ftpLocation} ; then
    mkdir -p ${dirName}/${ftpLocation}
fi

count=$(ls ${datasetDirName} | wc -l)

if [[ ${count} =~ 0 ]]; then
    echo "file not found!"
    echo "No data was found that satisfied the criteria you entered. Please try again." > README.txt
fi

#zip ${dirName}/${ftpLocation}/${archiveFileName} ./*
tar zcvf ${dirName}/${ftpLocation}/${archiveFileName} ./*

# Delete uncompressed raw data set
rm -rf ${datasetDirName}/*

# TODO: send the archived data to ftp server
# echo Need to ftp server information.
# echo ${ftpLocation}