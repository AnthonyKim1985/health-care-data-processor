package org.bigdatacenter.dataprocessor.platform.persistence.hive;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;

/**
 * Created by Anthony Jinhyuk Kim on 2017-05-30.
 */
@Mapper
public interface HiveMapper {
    void extractDataByHiveQL(@Param("hiveExtractionTask") HiveExtractionTask hiveExtractionTask);

    void createTableByHiveQL(@Param("hiveCreationTask") HiveCreationTask hiveCreationTask);
}