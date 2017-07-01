package org.bigdatacenter.dataprocessor.platform.persistence.metadb.version2;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestIndicatorInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestYearInfo;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
@Mapper
public interface MetadbVersion2Mapper {
    //
    // Request processing methods
    //
    RequestInfo readRequest(@Param("dataSetUID") Integer dataSetUID);

    List<RequestFilterInfo> readRequestFilters(@Param("dataSetUID") Integer dataSetUID);

    List<RequestYearInfo> readRequestYears(@Param("dataSetUID") Integer dataSetUID);

    List<RequestIndicatorInfo> readRequestIndicators(@Param("dataSetUID") Integer dataSetUID);


    //
    // FTP Request Meta Database
    //
    void createFtpRequest(@Param("ftpInfo") FtpInfo ftpInfo);

    FtpInfo readFtpRequest(@Param("dataSetUID") Integer dataSetUID);
}
