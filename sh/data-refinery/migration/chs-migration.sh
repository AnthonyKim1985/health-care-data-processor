#!/usr/bin/env bash
hive -e "load data local inpath '/home/hadoop/hyuk0628/raw_data/chs/chs_2008' overwrite into table chs.chs_2008;"
hive -e "load data local inpath '/home/hadoop/hyuk0628/raw_data/chs/chs_2009' overwrite into table chs.chs_2009;"
hive -e "load data local inpath '/home/hadoop/hyuk0628/raw_data/chs/chs_2010' overwrite into table chs.chs_2010;"
hive -e "load data local inpath '/home/hadoop/hyuk0628/raw_data/chs/chs_2011' overwrite into table chs.chs_2011;"
hive -e "load data local inpath '/home/hadoop/hyuk0628/raw_data/chs/chs_2012' overwrite into table chs.chs_2012;"
hive -e "load data local inpath '/home/hadoop/hyuk0628/raw_data/chs/chs_2013' overwrite into table chs.chs_2013;"
hive -e "load data local inpath '/home/hadoop/hyuk0628/raw_data/chs/chs_2014' overwrite into table chs.chs_2014;"
hive -e "load data local inpath '/home/hadoop/hyuk0628/raw_data/chs/chs_2015' overwrite into table chs.chs_2015;"