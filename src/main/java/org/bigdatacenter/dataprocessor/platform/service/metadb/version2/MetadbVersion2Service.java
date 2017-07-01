package org.bigdatacenter.dataprocessor.platform.service.metadb.version2;

import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestIndicatorInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestYearInfo;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
public interface MetadbVersion2Service {
    RequestInfo findRequest(Integer dataSetUID);

    List<RequestFilterInfo> findRequestFilters(Integer dataSetUID);

    List<RequestYearInfo> findRequestYears(Integer dataSetUID);

    List<RequestIndicatorInfo> findRequestIndicators(Integer dataSetUID);

    void insertFtpRequest(FtpInfo ftpInfo);

    boolean isExecutedJob(Integer dataSetUID);
}
