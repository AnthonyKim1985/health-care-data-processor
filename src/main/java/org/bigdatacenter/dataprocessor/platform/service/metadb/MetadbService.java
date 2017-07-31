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

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
public interface MetadbService {
    RequestInfo findRequest(Integer dataSetUID);

    List<RequestFilterInfo> findRequestFilters(Integer dataSetUID);

    List<RequestYearInfo> findRequestYears(Integer dataSetUID);

    List<RequestIndicatorInfo> findRequestIndicators(Integer dataSetUID);

    int updateProcessState(Integer dataSetUID, Integer processState);

    int updateJobStartTime(Integer dataSetUID, String jobStartTime);

    int updateJobEndTime(Integer dataSetUID, String jobEndTime);

    int updateElapsedTime(Integer dataSetUID, String elapsedTime);

    MetaDatabaseInfo findMetaDatabase(Integer edlIdx);

    MetaTableInfo findMetaTable(Integer etlIdx);

    List<String> findMetaTableNames(Integer edlIdx, Integer tbYear);

    List<MetaColumnInfo> findMetaColumns(Integer eclIdx);

    List<MetaColumnInfo> findMetaColumns(Integer edlIdx, String eclRef, Integer eclYear);

    List<String> findEngColumnNames(String etlEngName);

    List<MetaRelationIndicatorWithColumn> findMetaRelationIndicatorWithColumn(Integer eilIdx);

    List<MetaColumnInfo> findMetaColumnsForIndicatorHeader(Integer eilIdx);

    void insertFtpRequest(FtpInfo ftpInfo);

    boolean isExecutedJob(Integer dataSetUID);
}
