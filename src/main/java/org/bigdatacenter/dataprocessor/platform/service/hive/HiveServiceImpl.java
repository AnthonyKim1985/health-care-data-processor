package org.bigdatacenter.dataprocessor.platform.service.hive;

import org.bigdatacenter.dataprocessor.platform.domain.hive.HiveTask;
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
    public void extractDataByHiveQL(HiveTask hiveTask) {
        hiveMapper.extractDataByHiveQL(hiveTask.getHdfsLocation(), hiveTask.getHiveQuery());
    }
}
