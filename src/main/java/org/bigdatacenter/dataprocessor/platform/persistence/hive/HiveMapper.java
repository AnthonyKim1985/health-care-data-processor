package org.bigdatacenter.dataprocessor.platform.persistence.hive;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * Created by hyuk0 on 2017-05-30.
 */
@Mapper
public interface HiveMapper {
    void extractDataByHiveQL(@Param("hdfsLocation") String hdfsLocation, @Param("hiveQuery") String hiveQuery);
}