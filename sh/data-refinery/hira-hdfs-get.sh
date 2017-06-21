#!/usr/bin/env bash
for rawDataSetDir in $(hdfs dfs -ls /home/hadoop/hira_r2/);
do
	if [[ ${rawDataSetDir} =~ ^/ ]]; then
		dirName=`echo ${rawDataSetDir} | cut -d'/' -f5 | tr A-Z a-z`

		for processed in $(cat processed);
		do
			if [[ ${dirName} = ${processed} ]]; then
				continue 2
			fi
		done

		echo "[`date`][INFO] ${dirName} started."

		mkdir ${dirName}
		mkdir ${dirName}-Refined

		hdfs dfs -get ${rawDataSetDir}/* ${dirName}/

		for file in $(ls ${dirName});
		do
			sed '1d' ${dirName}/${file} > ${dirName}-Refined/${file}
		done

		find ${dirName}-Refined/* -name '_SUCCESS' -exec rm -rf {} \;
		find ${dirName}-Refined/* -size 0 -exec rm -rf {} \;

		ls ${dirName}-Refined/* | sort | xargs cat > ${dirName}.csv
		ssh dev01 -lhadoop "mkdir /home/hadoop/workspace/hira_import_to_hive/${dirName}"
		scp ${dirName}.csv hadoop@mongo01:/home/hadoop/workspace/hira_import_to_hive/${dirName}

		rm -rf ${dirName}
		rm -rf ${dirName}-Refined
		rm -rf ${dirName}.csv

		echo "[`date`][INFO] ${dirName} completed."
	fi
done

echo "[`date`][INFO] All job is done."