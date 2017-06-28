package org.bigdatacenter.dataprocessor.platform.persistence.hive;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.bigdatacenter.dataprocessor.platform.domain.hive.HiveTask;

/**
 * Created by Anthony Jinhyuk Kim on 2017-05-30.
 */
@Mapper
public interface HiveMapper {
    void extractDataByHiveQL(@Param("hiveTask") HiveTask hiveTask);
}