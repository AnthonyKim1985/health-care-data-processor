#!/usr/bin/env bash
for dirName in $(ls /home/hadoop/workspace/hira_import_to_hive);
do
	if [[ ${dirName} =~ (^aps|^nps|^nis|^pps) ]]; then
		echo "[`date`][INFO] Start to copy ${dirName} to nn1"
		ssh nn1 -l hadoop "mkdir /home/hadoop/workspace/hira_import_to_hive/$dirName"
		scp ${dirName}/${dirName}.csv hadoop@nn1:/home/hadoop/workspace/hira_import_to_hive/${dirName}

		echo "[`date`][INFO] Start to load ${dirName} to hive"
		dbName=`echo ${dirName} | cut -d'_' -f1`
		ssh nn1 -l hadoop "hive -e \"load data local inpath '/home/hadoop/workspace/hira_import_to_hive/${dirName}' overwrite into table ${dbName}.${dirName};\""

		echo "[`date`][INFO] clean up temporary file"
		ssh nn1 -l hadoop "rm -rf /home/hadoop/workspace/hira_import_to_hive/${dirName}"

		echo "[`date`][INFO] ${dirName} job completed"
	fi
done

echo "[`date`][INFO] All job is done."