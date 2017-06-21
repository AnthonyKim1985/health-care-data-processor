#!/usr/bin/env bash
#USAGE: nhic-hdfs-get.sh [hdfs_target_list] [storage_location]

hdfs_target_list=$1
storage_location=$2
for rawDataSetDir in $(cat ${hdfs_target_list});
do
	if [[ ${rawDataSetDir} =~ ^/ ]]; then
		dirName=`echo ${rawDataSetDir} | cut -d'/' -f6 | tr A-Z a-z`
		dataSet=`echo ${dirName} | cut -d'_' -f2`
		dataYear=`echo ${dirName} | cut -d'_' -f3`

		fileName=nhic\_${dataSet}\_${dataYear}

		for processed in $(cat processed);
		do
			if [[ ${rawDataSetDir} = ${processed} ]]; then
				continue 2
			fi
		done

		echo "[`date`][INFO] ${fileName} started."

		mkdir ${fileName}
		mkdir ${fileName}-Refined

		hdfs dfs -get ${rawDataSetDir}/* ${fileName}/

		for file in $(ls ${fileName});
		do
			sed '1d' ${fileName}/${file} > ${fileName}-Refined/${file}
		done

		find ${fileName}-Refined/* -name '_SUCCESS' -exec rm -rf {} \;
		find ${fileName}-Refined/* -size 0 -exec rm -rf {} \;

		ls ${fileName}-Refined/* | sort | xargs cat > ${fileName}.csv
		ssh dev01 -lhadoop "mkdir -p /$storage_location/nhic/$fileName"
		scp ${fileName}.csv hadoop@mongo01:/${storage_location}/nhic/${fileName}

		rm -rf ${fileName}
		rm -rf ${fileName}-Refined
		rm -rf ${fileName}.csv

		echo "[`date`][INFO] $fileName completed."
	fi
done

echo "[`date`][INFO] All job is done."