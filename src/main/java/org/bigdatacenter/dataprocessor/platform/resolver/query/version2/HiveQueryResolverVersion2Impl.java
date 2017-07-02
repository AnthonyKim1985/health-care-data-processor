package org.bigdatacenter.dataprocessor.platform.resolver.query.version2;

import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionParameterVersion2;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionRequestVersion2;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaDatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaRelationIndicatorWithColumn;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaTableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestIndicatorInfo;
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

        // TODO: find request
        RequestInfo requestInfo = metadbVersion2Service.findRequest(dataSetUID);
        if (requestInfo == null)
            return null;

        // TODO: find request year
        List<RequestYearInfo> requestYearInfoList = metadbVersion2Service.findRequestYears(dataSetUID);
        if (requestYearInfoList == null)
            return null;

        // TODO: find request indicator
        String indicator = takeIndicatorTask(dataSetUID);

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
            for (RequestFilterInfo requestFilterInfo : requestFilterInfoList) {
                // TODO: find column
                List<MetaColumnInfo> metaColumnInfoList = metadbVersion2Service.findMetaColumns(
                        requestInfo.getDatasetID(), requestFilterInfo.getFilterEngName(), Integer.parseInt(requestYearInfo.getYearName()));

                for (MetaColumnInfo metaColumnInfo : metaColumnInfoList) {
                    MetaTableInfo metaTableInfo = metadbVersion2Service.findMetaTable(metaColumnInfo.getEtl_idx());
                    if (metaTableInfo == null)
                        continue;

                    String filterValues = requestFilterInfo.getFilterValues();
                    if (filterValues == null)
                        return null;

                    for (String value : filterValues.split("[,]"))
                        taskInfoList.add(new TaskInfo(metaDatabaseInfo.getEdl_eng_name(), metaTableInfo.getEtl_eng_name(), metaColumnInfo.getEcl_eng_name(), value));
                }
            }
        }

        return new ExtractionParameterVersion2(requestInfo, indicator, HiveQueryResolverUtil.convertTaskInfoListToParameterMap(taskInfoList));
    }

    private String takeIndicatorTask(Integer dataSetUID) {
        Map<String, Object> indicatorMap = new HashMap<>();
        List<String> indicatorList = new ArrayList<>();

        List<RequestIndicatorInfo> requestIndicatorInfoList = metadbVersion2Service.findRequestIndicators(dataSetUID);

        for (RequestIndicatorInfo requestIndicatorInfo : requestIndicatorInfoList) {
            List<MetaRelationIndicatorWithColumn> relationIndicatorWithColumnList = metadbVersion2Service.findMetaRelationIndicatorWithColumn(requestIndicatorInfo.getIndicatorID());

            for (MetaRelationIndicatorWithColumn relationIndicatorWithColumn : relationIndicatorWithColumnList) {
                List<MetaColumnInfo> columnInfoList = metadbVersion2Service.findMetaColumns(relationIndicatorWithColumn.getEcl_idx());

                for (MetaColumnInfo columnInfo : columnInfoList)
                    if (!indicatorMap.containsKey(columnInfo.getEcl_eng_name()))
                        indicatorMap.put(columnInfo.getEcl_eng_name(), null);
            }
        }

        indicatorList.addAll(indicatorMap.keySet());

        StringBuilder indicatorBuilder = new StringBuilder();
        for (int i = 0; i < indicatorList.size(); i++) {
            indicatorBuilder.append(indicatorList.get(i));
            if (i < indicatorList.size() - 1)
                indicatorBuilder.append(',');
        }

        if (indicatorBuilder.toString().length() == 0)
            return null;

        return indicatorBuilder.toString();
    }

    @Override
    public ExtractionRequestVersion2 buildExtractionRequest(ExtractionParameterVersion2 extractionParameter) {
        RequestInfo requestInfo = extractionParameter.getRequestInfo();
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = extractionParameter.getParameterMap();
        List<HiveTask> hiveTaskList = HiveQueryResolverUtil.convertParameterMapToHiveTaskList(requestInfo.getDataSetUID(), parameterMap, extractionParameter.getIndicator());

        return new ExtractionRequestVersion2(requestInfo, hiveTaskList);
    }
}