package org.bigdatacenter.dataprocessor.platform.service.metadb.version1;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.ColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.DatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.TableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.FilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.persistence.metadb.version1.MetadbVersion1Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-28.
 */
@Service
@Deprecated
public class MetadbVersion1ServiceImpl implements MetadbVersion1Service {
    @Autowired
    private MetadbVersion1Mapper metadbVersion1Mapper;

    @Override
    public RequestInfo findRequest(Integer dataSetUID) {
        return metadbVersion1Mapper.readRequest(dataSetUID);
    }

    @Override
    public List<FilterInfo> findConditions(Integer dataSetUID) {
        return metadbVersion1Mapper.readConditions(dataSetUID);
    }

    @Override
    public List<ColumnInfo> findColumnInfo(String eclEngName) {
        return metadbVersion1Mapper.readColumnInfo(eclEngName);
    }

    @Override
    public TableInfo findTableInfo(Integer etlIdx) {
        return metadbVersion1Mapper.readTableInfo(etlIdx);
    }

    @Override
    public DatabaseInfo findDatabaseInfo(Integer edlIdx) {
        return metadbVersion1Mapper.readDatabaseInfo(edlIdx);
    }

    @Override
    public void insertFtpRequest(FtpInfo ftpInfo) {
        metadbVersion1Mapper.createFtpRequest(ftpInfo);
    }

    @Override
    public boolean isExecutedJob(Integer dataSetUID) {
        return metadbVersion1Mapper.readFtpRequest(dataSetUID) != null;
    }
}
