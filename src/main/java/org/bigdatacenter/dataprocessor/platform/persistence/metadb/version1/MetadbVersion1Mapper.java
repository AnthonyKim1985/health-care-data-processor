package org.bigdatacenter.dataprocessor.platform.persistence.metadb.version1;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
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
@Mapper
@Deprecated
public interface MetadbVersion1Mapper {
    //
    // Bogun Database
    //
    RequestInfo readRequest(@Param("dataSetUID") Integer dataSetUID);

    List<RequestFilterInfo> readFilters(@Param("dataSetUID") Integer dataSetUID);

    //
    // Extraction Database
    //
    List<MetaColumnInfo> readColumnInfo(@Param("eclEngName") String eclEngName);

    MetaTableInfo readTableInfo(@Param("etlIdx") Integer etlIdx);

    MetaDatabaseInfo readDatabaseInfo(@Param("edlIdx") Integer edlIdx);

    //
    // FTP Request Meta Database
    //
    void createFtpRequest(@Param("ftpInfo") FtpInfo ftpInfo);

    FtpInfo readFtpRequest(@Param("dataSetUID") Integer dataSetUID);
}