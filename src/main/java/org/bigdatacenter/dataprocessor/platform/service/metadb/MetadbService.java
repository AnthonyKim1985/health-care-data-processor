package org.bigdatacenter.dataprocessor.platform.service.metadb;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.*;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-08.
 */
public interface MetadbService {
    RequestInfo findRequest(Integer dataSetUID);

    List<ConditionInfo> findConditions(Integer dataSetUID);

    List<ColumnInfo> findColumnInfo(String eclEngName);

    TableInfo findTableInfo(Integer etlIdx);

    DatabaseInfo findDatabaseInfo(Integer edlIdx);

    void insertFtpRequest(FtpInfo ftpInfo);

    boolean isExecutedJob(Integer dataSetUID);
}
