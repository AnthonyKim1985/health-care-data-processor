package org.bigdatacenter.dataprocessor.platform.service.metadb;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaDatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaRelationIndicatorWithColumn;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaTableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestIndicatorInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestYearInfo;
import org.bigdatacenter.dataprocessor.platform.persistence.metadb.MetadbMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
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
    public List<RequestFilterInfo> findRequestFilters(Integer dataSetUID) {
        return metadbMapper.readRequestFilters(dataSetUID);
    }

    @Override
    public List<RequestYearInfo> findRequestYears(Integer dataSetUID) {
        return metadbMapper.readRequestYears(dataSetUID);
    }

    @Override
    public List<RequestIndicatorInfo> findRequestIndicators(Integer dataSetUID) {
        return metadbMapper.readRequestIndicators(dataSetUID);
    }

    @Override
    public int updateProcessState(Integer dataSetUID, Integer processState) {
        return metadbMapper.updateProcessState(dataSetUID, processState);
    }

    @Override
    public int updateJobStartTime(Integer dataSetUID, String jobStartTime) {
        return metadbMapper.updateJobStartTime(dataSetUID, jobStartTime);
    }

    @Override
    public int updateJobEndTime(Integer dataSetUID, String jobEndTime) {
        return metadbMapper.updateJobEndTime(dataSetUID, jobEndTime);
    }

    @Override
    public int updateElapsedTime(Integer dataSetUID, String elapsedTime) {
        return metadbMapper.updateElapsedTime(dataSetUID, elapsedTime);
    }

    @Override
    public MetaDatabaseInfo findMetaDatabase(Integer edlIdx) {
        return metadbMapper.readMetaDatabase(edlIdx);
    }

    @Override
    public MetaTableInfo findMetaTable(Integer etlIdx) {
        return metadbMapper.readMetaTable(etlIdx);
    }

    @Override
    public List<String> findMetaTableNames(Integer edlIdx, Integer tbYear) {
        return metadbMapper.readMetaTableNames(edlIdx, tbYear);
    }

    @Override
    public List<MetaColumnInfo> findMetaColumns(Integer eclIdx) {
        return metadbMapper.readMetaColumns1(eclIdx);
    }

    @Override
    public List<MetaColumnInfo> findMetaColumns(Integer edlIdx, String eclRef, Integer eclYear) {
        return metadbMapper.readMetaColumns2(edlIdx, eclRef, eclYear);
    }

    @Override
    public List<String> findEngColumnNames(String etlEngName) {
        return metadbMapper.readEngColumnNames(etlEngName);
    }

    @Override
    public List<MetaRelationIndicatorWithColumn> findMetaRelationIndicatorWithColumn(Integer eilIdx) {
        return metadbMapper.readMetaRelationIndicatorWithColumn(eilIdx);
    }

    @Override
    public List<MetaColumnInfo> findMetaColumnsForIndicatorHeader(Integer eilIdx) {
        return metadbMapper.readMetaColumnsForIndicatorHeader(eilIdx);
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