package org.bigdatacenter.dataprocessor.platform.service.hive;

import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;
import org.bigdatacenter.dataprocessor.platform.persistence.hive.HiveMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-28.
 */
@Service
public class HiveServiceImpl implements HiveService {
    @Autowired
    private HiveMapper hiveMapper;

    @Override
    public void extractDataByHiveQL(HiveExtractionTask hiveExtractionTask) {
        hiveMapper.extractDataByHiveQL(hiveExtractionTask);
    }

    @Override
    public void createTableByHiveQL(HiveCreationTask hiveCreationTask) {
        hiveMapper.createTableByHiveQL(hiveCreationTask);
    }
}
