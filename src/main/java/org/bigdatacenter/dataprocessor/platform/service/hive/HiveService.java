package org.bigdatacenter.dataprocessor.platform.service.hive;

import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;

/**
 * Created by Anthony Jinhyuk Kim on 2017-05-30.
 */
public interface HiveService {
    void extractDataByHiveQL(HiveExtractionTask hiveExtractionTask);

    void createTableByHiveQL(HiveCreationTask hiveCreationTask);
}