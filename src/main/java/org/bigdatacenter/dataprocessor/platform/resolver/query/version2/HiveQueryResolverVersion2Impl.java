package org.bigdatacenter.dataprocessor.platform.resolver.query.version2;

import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionParameterVersion2;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionRequestVersion2;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaDatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaTableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestYearInfo;
import org.bigdatacenter.dataprocessor.platform.resolver.query.common.HiveQueryResolverUtil;
import org.bigdatacenter.dataprocessor.platform.service.metadb.version2.MetadbVersion2Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class HiveQueryResolverVersion2Impl implements HiveQueryResolverVersion2 {
    private static final Logger logger = LoggerFactory.getLogger(HiveQueryResolverVersion2Impl.class);
    private final String currentThreadName = Thread.currentThread().getName();

    @Autowired
    private MetadbVersion2Service metadbVersion2Service;

    @Override
    public ExtractionParameterVersion2 buildExtractionParameter(Integer dataSetUID) {
        List<TaskInfo> taskInfoList = new ArrayList<>();
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = new HashMap<>();

        // TODO: find request
        RequestInfo requestInfo = metadbVersion2Service.findRequest(dataSetUID);
        if (requestInfo == null)
            return null;

        // TODO: find request year
        List<RequestYearInfo> requestYearInfoList = metadbVersion2Service.findRequestYears(dataSetUID);
        if (requestYearInfoList == null)
            return null;

        // TODO: find request filters
        List<RequestFilterInfo> requestFilterInfoList = metadbVersion2Service.findRequestFilters(dataSetUID);
        if (requestFilterInfoList == null)
            return null;

        // TODO: find database
        MetaDatabaseInfo metaDatabaseInfo = metadbVersion2Service.findMetaDatabase(requestInfo.getDatasetID());
        if (metaDatabaseInfo == null)
            return null;

        // TODO: make tasks
        for (RequestYearInfo requestYearInfo : requestYearInfoList) {
            logger.debug(requestYearInfo.toString());
            for (RequestFilterInfo requestFilterInfo : requestFilterInfoList) {
                logger.debug(requestFilterInfo.toString());

                // TODO: find column
                List<MetaColumnInfo> metaColumnInfoList = metadbVersion2Service.findMetaColumns(
                        requestInfo.getDatasetID(), requestFilterInfo.getFilterEngName(), Integer.parseInt(requestYearInfo.getYearName()));

                logger.debug(metaColumnInfoList.toString());

                for (MetaColumnInfo metaColumnInfo : metaColumnInfoList) {
                    logger.debug(metaColumnInfo.toString());
                    MetaTableInfo metaTableInfo = metadbVersion2Service.findMetaTable(metaColumnInfo.getEtl_idx());

                    if (metaTableInfo == null)
                        continue;

                    logger.debug(metaTableInfo.toString());

                    String filterValues = requestFilterInfo.getFilterValues();
                    if (filterValues == null)
                        return null;

                    for (String value : filterValues.split("[,]"))
                        taskInfoList.add(new TaskInfo(metaDatabaseInfo.getEdl_eng_name(), metaTableInfo.getEtl_eng_name(), metaColumnInfo.getEcl_eng_name(), value));
                }
            }
        }

        logger.debug("buildExtractionParameter is done");

        return new ExtractionParameterVersion2(requestInfo, HiveQueryResolverUtil.convertTaskInfoListToParameterMap(taskInfoList));
    }

    @Override
    public ExtractionRequestVersion2 buildExtractionRequest(ExtractionParameterVersion2 extractionParameter) {
        RequestInfo requestInfo = extractionParameter.getRequestInfo();
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = extractionParameter.getParameterMap();
        List<HiveTask> hiveTaskList = HiveQueryResolverUtil.convertParameterMapToHiveTaskList(requestInfo.getDataSetUID(), parameterMap);

        return new ExtractionRequestVersion2(requestInfo, hiveTaskList);
    }
}
