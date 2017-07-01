package org.bigdatacenter.dataprocessor.platform.persistence.metadb.version1;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
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
@Mapper
@Deprecated
public interface MetadbVersion1Mapper {
    //
    // Bogun Database
    //
    RequestInfo readRequest(@Param("dataSetUID") Integer dataSetUID);

    List<FilterInfo> readConditions(@Param("dataSetUID") Integer dataSetUID);

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

    FtpInfo readFtpRequest(@Param("dataSetUID") Integer dataSetUID);
}