package org.bigdatacenter.dataprocessor.platform.service.metadb;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.*;
import org.bigdatacenter.dataprocessor.platform.persistence.metadb.MetadbMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-08.
 */
@Service
public class MetadbService {
    @Autowired
    private MetadbMapper metadbMapper;

    public RequestInfo findRequest(Integer dataSetUID) {
        return metadbMapper.readRequest(dataSetUID);
    }

    public List<ConditionInfo> findConditions(Integer dataSetUID) {
        return metadbMapper.readConditions(dataSetUID);
    }

    public List<ColumnInfo> findColumnInfo(String eclEngName) {
        return metadbMapper.readColumnInfo(eclEngName);
    }

    public TableInfo findTableInfo(Integer etlIdx) {
        return metadbMapper.readTableInfo(etlIdx);
    }

    public DatabaseInfo findDatabaseInfo(Integer edlIdx) {
        return metadbMapper.readDatabaseInfo(edlIdx);
    }

    public void insertFtpRequest(FtpInfo ftpInfo) {
        metadbMapper.createFtpRequest(ftpInfo);
    }

    public boolean isExecutedJob(Integer dataSetUID) {
        return metadbMapper.readFtpRequest(dataSetUID) != null;
    }
}
