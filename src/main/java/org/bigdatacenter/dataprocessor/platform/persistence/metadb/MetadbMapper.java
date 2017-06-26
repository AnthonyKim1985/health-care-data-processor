package org.bigdatacenter.dataprocessor.platform.persistence.metadb;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.*;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-08.
 */
@Mapper
public interface MetadbMapper {
    //
    // Bogun Database
    //
    RequestInfo readRequest(@Param("dataSetUID") Integer dataSetUID);

    List<ConditionInfo> readConditions(@Param("dataSetUID") Integer dataSetUID);

    //
    // Extraction Database
    //
    List<ColumnInfo> readColumnInfo(@Param("eclEngName") String eclEngName);

    TableInfo readTableInfo(@Param("etlIdx") Integer etlIdx);

    DatabaseInfo readDatabaseInfo(@Param("edlIdx") Integer edlIdx);

    //
    // FTP Request Meta Database
    //
    void createFtpRequest(@Param("ftpInfo") FtpInfo ftpInfo);
}