package org.bigdatacenter.dataprocessor.platform.service.metadb;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.*;
import org.bigdatacenter.dataprocessor.platform.persistence.metadb.MetadbMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by hyuk0 on 2017-06-08.
 */
@Service
public class MetadbService {
    @Autowired
    private MetadbMapper metadbMapper;

    public RequestInfo findRequest(Integer dataSetUID) {
        return metadbMapper.findRequest(dataSetUID);
    }

    public List<ConditionInfo> findConditions(Integer dataSetUID) {
        return metadbMapper.findConditions(dataSetUID);
    }

    public List<ColumnInfo> findColumnInfo(String eclEngName) {
        return metadbMapper.findColumnInfo(eclEngName);
    }

    public TableInfo findTableInfo(Integer etlIdx) {
        return metadbMapper.findTableInfo(etlIdx);
    }

    public DatabaseInfo findDatabaseInfo(Integer edlIdx) {
        return metadbMapper.findDatabaseInfo(edlIdx);
    }
}
