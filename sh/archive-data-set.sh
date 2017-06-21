#!/usr/bin/env bash
#USAGE: sh archive-data-set.sh [archiveFileName] [ftpLocation]

archiveFileName=$1
ftpLocation=$2

datasetDirName=/home/hadoop/hyuk0628/health-care-service/extracted_dataset
dirName=/home/hadoop/hyuk0628/health-care-service/archive

if ! test -d ${dirName} ; then
    mkdir ${dirName}
fi

find ${datasetDirName}/* -size 0 -exec rm -rf {} \;
cd ${datasetDirName}
zip ${dirName}/${archiveFileName}.zip ./*.csv

# Delete uncompressed raw data set
rm -rf ${datasetDirName}/*

# TODO: send the archived data to ftp server
echo Need to ftp server information.
echo ${ftpLocation}
