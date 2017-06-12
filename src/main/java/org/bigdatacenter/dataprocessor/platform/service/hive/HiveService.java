package org.bigdatacenter.dataprocessor.platform.service.hive;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.persistence.hive.HiveMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by hyuk0 on 2017-05-30.
 */
@Service
public class HiveService {
    @Autowired
    private HiveMapper hiveMapper;

    public void extractDataByHiveQL(ExtractionRequest extractionRequest) {
        hiveMapper.extractDataByHiveQL(extractionRequest.getHdfsLocation(), extractionRequest.getHiveQuery());
    }
}