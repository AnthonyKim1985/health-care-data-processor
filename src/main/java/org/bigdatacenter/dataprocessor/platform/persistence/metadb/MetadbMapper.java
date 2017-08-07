package org.bigdatacenter.dataprocessor.platform.persistence.metadb;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.*;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestIndicatorInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestYearInfo;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
@Mapper
public interface MetadbMapper {
    Integer PROCESS_STATE_COMPLETED = 1;
    Integer PROCESS_STATE_PROCESSING = 2;
    Integer PROCESS_STATE_REJECTED = 3;

    //
    // Request Transaction Database Methods
    //
    RequestInfo readRequest(@Param("dataSetUID") Integer dataSetUID);
    List<RequestFilterInfo> readRequestFilters(@Param("dataSetUID") Integer dataSetUID);
    List<RequestYearInfo> readRequestYears(@Param("dataSetUID") Integer dataSetUID);
    int updateProcessState(@Param("dataSetUID") Integer dataSetUID, @Param("processState") Integer processState);
    int updateJobStartTime(@Param("dataSetUID") Integer dataSetUID, @Param("jobStartTime") String jobStartTime);
    int updateJobEndTime(@Param("dataSetUID") Integer dataSetUID, @Param("jobEndTime") String jobEndTime);
    int updateElapsedTime(@Param("dataSetUID") Integer dataSetUID, @Param("elapsedTime") String elapsedTime);


    //
    // Meta Database Methods
    //
    MetaDatabaseInfo readMetaDatabase(@Param("edl_idx") Integer edlIdx);
    MetaTableInfo readMetaTable(@Param("etl_idx") Integer etlIdx);
    List<String> readMetaTableNames(@Param("edl_idx") Integer edlIdx, @Param("tb_year") Integer tbYear);
    List<MetaColumnInfo> readMetaColumns(@Param("edl_idx") Integer edlIdx, @Param("ecl_ref") String eclRef, @Param("ecl_year") Integer eclYear);
    List<String> readEngColumnNames(@Param("etl_eng_name") String etlEngName);
    List<MetaSelectedColumnInfo> readMetaSelectedColumns(@Param("dataSetUID") Integer dataSetUID, @Param("etl_eng_name") String etlEngName);


    //
    // FTP Request Meta Database
    //
    void createFtpRequest(@Param("ftpInfo") FtpInfo ftpInfo);
    FtpInfo readFtpRequest(@Param("dataSetUID") Integer dataSetUID);
}