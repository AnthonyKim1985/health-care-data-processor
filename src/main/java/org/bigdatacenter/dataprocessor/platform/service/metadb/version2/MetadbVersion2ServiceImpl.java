package org.bigdatacenter.dataprocessor.platform.service.metadb.version2;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaDatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaRelationIndicatorWithColumn;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaTableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestIndicatorInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestYearInfo;
import org.bigdatacenter.dataprocessor.platform.persistence.metadb.version2.MetadbVersion2Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
@Service
public class MetadbVersion2ServiceImpl implements MetadbVersion2Service {
    @Autowired
    private MetadbVersion2Mapper metadbVersion2Mapper;

    @Override
    public RequestInfo findRequest(Integer dataSetUID) {
        return metadbVersion2Mapper.readRequest(dataSetUID);
    }

    @Override
    public List<RequestFilterInfo> findRequestFilters(Integer dataSetUID) {
        return metadbVersion2Mapper.readRequestFilters(dataSetUID);
    }

    @Override
    public List<RequestYearInfo> findRequestYears(Integer dataSetUID) {
        return metadbVersion2Mapper.readRequestYears(dataSetUID);
    }

    @Override
    public List<RequestIndicatorInfo> findRequestIndicators(Integer dataSetUID) {
        return metadbVersion2Mapper.readRequestIndicators(dataSetUID);
    }

    @Override
    public MetaDatabaseInfo findMetaDatabase(Integer edlIdx) {
        return metadbVersion2Mapper.readMetaDatabase(edlIdx);
    }

    @Override
    public MetaTableInfo findMetaTable(Integer etlIdx) {
        return metadbVersion2Mapper.readMetaTable(etlIdx);
    }

    @Override
    public List<MetaColumnInfo> findMetaColumns(Integer eclIdx) {
        return metadbVersion2Mapper.readMetaColumns1(eclIdx);
    }

    @Override
    public List<MetaColumnInfo> findMetaColumns(Integer edlIdx, String eclEngName, Integer eclYear) {
        return metadbVersion2Mapper.readMetaColumns2(edlIdx, eclEngName, eclYear);
    }

    @Override
    public List<MetaRelationIndicatorWithColumn> findMetaRelationIndicatorWithColumn(Integer eilIdx) {
        return metadbVersion2Mapper.readMetaRelationIndicatorWithColumn(eilIdx);
    }

    @Override
    public void insertFtpRequest(FtpInfo ftpInfo) {
        metadbVersion2Mapper.createFtpRequest(ftpInfo);
    }

    @Override
    public boolean isExecutedJob(Integer dataSetUID) {
        return metadbVersion2Mapper.readFtpRequest(dataSetUID) != null;
    }
}