package org.bigdatacenter.dataprocessor.platform.service.metadb.version1;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.MetaColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.MetaDatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.MetaTableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.RequestInfo;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-08.
 */
@Deprecated
public interface MetadbVersion1Service {
    RequestInfo findRequest(Integer dataSetUID);

    List<RequestFilterInfo> findConditions(Integer dataSetUID);

    List<MetaColumnInfo> findColumnInfo(String eclEngName);

    MetaTableInfo findTableInfo(Integer etlIdx);

    MetaDatabaseInfo findDatabaseInfo(Integer edlIdx);

    void insertFtpRequest(FtpInfo ftpInfo);

    boolean isExecutedJob(Integer dataSetUID);
}
