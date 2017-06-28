package org.bigdatacenter.dataprocessor.platform.service.metadb;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.*;
import org.bigdatacenter.dataprocessor.platform.persistence.metadb.MetadbMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-28.
 */
@Service
public class MetadbServiceImpl implements MetadbService {
    @Autowired
    private MetadbMapper metadbMapper;

    @Override
    public RequestInfo findRequest(Integer dataSetUID) {
        return metadbMapper.readRequest(dataSetUID);
    }

    @Override
    public List<ConditionInfo> findConditions(Integer dataSetUID) {
        return metadbMapper.readConditions(dataSetUID);
    }

    @Override
    public List<ColumnInfo> findColumnInfo(String eclEngName) {
        return metadbMapper.readColumnInfo(eclEngName);
    }

    @Override
    public TableInfo findTableInfo(Integer etlIdx) {
        return metadbMapper.readTableInfo(etlIdx);
    }

    @Override
    public DatabaseInfo findDatabaseInfo(Integer edlIdx) {
        return metadbMapper.readDatabaseInfo(edlIdx);
    }

    @Override
    public void insertFtpRequest(FtpInfo ftpInfo) {
        metadbMapper.createFtpRequest(ftpInfo);
    }

    @Override
    public boolean isExecutedJob(Integer dataSetUID) {
        return metadbMapper.readFtpRequest(dataSetUID) != null;
    }
}
