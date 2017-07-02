package org.bigdatacenter.dataprocessor.platform.service.hive;

import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;

/**
 * Created by Anthony Jinhyuk Kim on 2017-05-30.
 */
public interface HiveService {
    void extractDataByHiveQL(HiveTask hiveTask);
}