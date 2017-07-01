package org.bigdatacenter.dataprocessor.platform.service.metadb.version1;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.ColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.DatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.TableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.FilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.RequestInfo;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-08.
 */
@Deprecated
public interface MetadbVersion1Service {
    RequestInfo findRequest(Integer dataSetUID);

    List<FilterInfo> findConditions(Integer dataSetUID);

    List<ColumnInfo> findColumnInfo(String eclEngName);

    TableInfo findTableInfo(Integer etlIdx);

    DatabaseInfo findDatabaseInfo(Integer edlIdx);

    void insertFtpRequest(FtpInfo ftpInfo);

    boolean isExecutedJob(Integer dataSetUID);
}
