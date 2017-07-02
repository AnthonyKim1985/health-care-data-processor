package org.bigdatacenter.dataprocessor.platform.resolver.query.version2;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestYearInfo;
import org.bigdatacenter.dataprocessor.platform.resolver.query.common.HiveQueryResolver;
import org.bigdatacenter.dataprocessor.platform.service.metadb.version2.MetadbVersion2Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
@Component
@Qualifier("HiveQueryResolverVersion2Impl")
public class HiveQueryResolverVersion2Impl extends HiveQueryResolver {
    @Autowired
    private MetadbVersion2Service metadbVersion2Service;

    @Override
    public ExtractionParameter buildExtractionParameter(Integer dataSetUID) {
        List<TaskInfo> taskInfoList = new ArrayList<>();
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = new HashMap<>();

        RequestInfo requestInfo = metadbVersion2Service.findRequest(dataSetUID);
        if (requestInfo == null)
            return null;

        List<RequestYearInfo> requestYearInfoList = metadbVersion2Service.findRequestYears(dataSetUID);
        if (requestYearInfoList == null)
            return null;

        List<RequestFilterInfo> requestFilterInfoList = metadbVersion2Service.findRequestFilters(dataSetUID);
        if (requestFilterInfoList == null)
            return null;

        for (RequestYearInfo requestYearInfo : requestYearInfoList) {
            for (RequestFilterInfo requestFilterInfo : requestFilterInfoList) {

            }
        }

        return null;
    }
}
