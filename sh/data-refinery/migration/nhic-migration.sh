#!/usr/bin/env bash

#nhic_t30
for dirName in $(ls /bigdata1/nhic_t30/nhic);
do
	if [[ ${dirName} =~ ^nhic ]]; then
		echo "[`date`][INFO] Start to copy ${dirName} to nn1"
		ssh nn1 -l hadoop "mkdir /home/hadoop/workspace/nhic_import_to_hive/${dirName}"
		scp /bigdata1/nhic_t30/nhic/${dirName}/${dirName}.csv hadoop@nn1:/home/hadoop/workspace/nhic_import_to_hive/${dirName}

		echo "[`date`][INFO] Start to load ${dirName} to hive"
		dbName=`echo ${dirName} | cut -d'_' -f1`
		ssh nn1 -l hadoop "hive -e \"load data local inpath '/home/hadoop/workspace/nhic_import_to_hive/${dirName}' overwrite into table ${dbName}.${dirName};\""

		echo "[`date`][INFO] clean up temporary file"
		ssh nn1 -l hadoop "rm -rf /home/hadoop/workspace/nhic_import_to_hive/${dirName}"

		echo "[`date`][INFO] ${dirName} job completed"
	fi
done

#nhic_others
for dirName in $(ls /bigdata2/nhic_others/nhic);
do
	if [[ ${dirName} =~ ^nhic ]]; then
		echo "[`date`][INFO] Start to copy ${dirName} to nn1"
		ssh nn1 -l hadoop "mkdir /home/hadoop/workspace/nhic_import_to_hive/${dirName}"
		scp /bigdata2/nhic_others/nhic/${dirName}.csv hadoop@nn1:/home/hadoop/workspace/nhic_import_to_hive/${dirName}

		echo "[`date`][INFO] Start to load ${dirName} to hive"
		dbName=`echo ${dirName} | cut -d'_' -f1`
		ssh nn1 -l hadoop "hive -e \"load data local inpath '/home/hadoop/workspace/nhic_import_to_hive/${dirName}' overwrite into table ${dbName}.${dirName};\""

		echo "[`date`][INFO] clean up temporary file"
		ssh nn1 -l hadoop "rm -rf /home/hadoop/workspace/nhic_import_to_hive/${dirName}"

		echo "[`date`][INFO] ${dirName} job completed"
	fi
done

echo "[`date`][INFO] All job is done."