package org.bigdatacenter.dataprocessor.platform.resolver.query;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaDatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaRelationIndicatorWithColumn;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaTableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestIndicatorInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestYearInfo;
import org.bigdatacenter.dataprocessor.platform.service.metadb.MetadbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
@Component
public class HiveQueryResolverImpl implements HiveQueryResolver {
    private static final Logger logger = LoggerFactory.getLogger(HiveQueryResolverImpl.class);
    private final String currentThreadName = Thread.currentThread().getName();

    @Autowired
    private MetadbService metadbService;

    @Override
    public ExtractionParameter buildExtractionParameter(Integer dataSetUID) {
        List<TaskInfo> taskInfoList = new ArrayList<>();

        // TODO: find request
        RequestInfo requestInfo = metadbService.findRequest(dataSetUID);
        if (requestInfo == null)
            return null;

        // TODO: find request year
        List<RequestYearInfo> requestYearInfoList = metadbService.findRequestYears(dataSetUID);
        if (requestYearInfoList == null)
            return null;

        // TODO: find request filters
        List<RequestFilterInfo> requestFilterInfoList = metadbService.findRequestFilters(dataSetUID);
        if (requestFilterInfoList == null)
            return null;

        // TODO: find request indicator
        String indicator = takeIndicatorTask(dataSetUID);

        // TODO: find database
        MetaDatabaseInfo metaDatabaseInfo = metadbService.findMetaDatabase(requestInfo.getDatasetID());
        if (metaDatabaseInfo == null)
            return null;

        // TODO: make tasks
        for (RequestYearInfo requestYearInfo : requestYearInfoList) {
            for (RequestFilterInfo requestFilterInfo : requestFilterInfoList) {
                // TODO: find column
                List<MetaColumnInfo> metaColumnInfoList = metadbService.findMetaColumns(
                        requestInfo.getDatasetID(), requestFilterInfo.getFilterEngName(), Integer.parseInt(requestYearInfo.getYearName()));

                for (MetaColumnInfo metaColumnInfo : metaColumnInfoList) {
                    MetaTableInfo metaTableInfo = metadbService.findMetaTable(metaColumnInfo.getEtl_idx());
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

        return new ExtractionParameter(requestInfo, indicator, convertTaskInfoListToParameterMap(taskInfoList));
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        RequestInfo requestInfo = extractionParameter.getRequestInfo();
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = extractionParameter.getParameterMap();
        List<HiveTask> hiveTaskList = convertParameterMapToHiveTaskList(requestInfo.getDataSetUID(), parameterMap, extractionParameter.getIndicator());

        if (hiveTaskList == null)
            return null;

        return new ExtractionRequest(requestInfo, extractionParameter.getIndicator(), hiveTaskList);
    }

    private String takeIndicatorTask(Integer dataSetUID) {
        List<RequestIndicatorInfo> requestIndicatorInfoList = metadbService.findRequestIndicators(dataSetUID);

        StringBuilder indicatorBuilder = new StringBuilder();
        Map<String, Object> indicatorMap = new HashMap<>();

        for (RequestIndicatorInfo requestIndicatorInfo : requestIndicatorInfoList) {
            List<MetaRelationIndicatorWithColumn> relationIndicatorWithColumnList = metadbService.findMetaRelationIndicatorWithColumn(requestIndicatorInfo.getIndicatorID());

            for (MetaRelationIndicatorWithColumn relationIndicatorWithColumn : relationIndicatorWithColumnList) {
                List<MetaColumnInfo> columnInfoList = metadbService.findMetaColumns(relationIndicatorWithColumn.getEcl_idx());

                for (MetaColumnInfo columnInfo : columnInfoList)
                    if (!indicatorMap.containsKey(columnInfo.getEcl_eng_name()))
                        indicatorMap.put(columnInfo.getEcl_eng_name(), null);
            }
        }

        List<String> headerList = new ArrayList<>(indicatorMap.keySet());
        for (int i = 0; i < headerList.size(); i++) {
            indicatorBuilder.append(headerList.get(i));
            if (i < headerList.size() - 1)
                indicatorBuilder.append(',');
        }

        if (indicatorBuilder.toString().length() == 0)
            return null;

        return indicatorBuilder.toString();
    }

    private Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> convertTaskInfoListToParameterMap(List<TaskInfo> taskInfoList) {
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = new HashMap<>();

        for (TaskInfo taskInfo : taskInfoList) {
            String parameterKey = String.format("%s.%s", taskInfo.getDatabaseName(), taskInfo.getTableName());
            Map<String/*column*/, List<String>/*values*/> parameterValue = parameterMap.get(parameterKey);

            List<String> values;
            if (parameterValue == null) {
                values = new ArrayList<>();
                values.add(taskInfo.getValue());

                parameterValue = new HashMap<>();
                parameterValue.put(taskInfo.getColumnName(), values);

                parameterMap.put(parameterKey, parameterValue);
            } else {
                values = parameterValue.get(taskInfo.getColumnName());

                if (values == null) {
                    values = new ArrayList<>();
                    values.add(taskInfo.getValue());

                    parameterValue.put(taskInfo.getColumnName(), values);
                } else {
                    values.add(taskInfo.getValue());
                }
            }
        }
        return parameterMap;
    }

    private List<HiveTask> convertParameterMapToHiveTaskList(Integer dataSetUID, Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap, String indicatorHeader) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        for (String dbAndTableName : parameterMap.keySet()) {
            if (dbAndTableName.startsWith("nps.nps_ykiho"))
                continue;

            StringBuilder hiveQueryBuilder = new StringBuilder();
            String header = indicatorHeader == null ? getHeader(dbAndTableName) : indicatorHeader;
            if (indicatorHeader == null)
                return null;

            hiveQueryBuilder.append(String.format("SELECT %s FROM %s", header, dbAndTableName));

            Map<String/*column*/, List<String>/*values*/> conditionMap = parameterMap.get(dbAndTableName);

            List<String> columnNameList = new ArrayList<>();
            columnNameList.addAll(conditionMap.keySet());

            if (columnNameList.size() > 0)
                hiveQueryBuilder.append(buildWhereClause(columnNameList, conditionMap));

            // /tmp/health_care/{dataSetUID}/{dbAndTableName}/{timeStamp}
            final String hdfsLocation = String.format("/tmp/health_care/%d/%s/%s", dataSetUID,
                    dbAndTableName, String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
            hiveTaskList.add(new HiveTask(hdfsLocation, hiveQueryBuilder.toString(), header));
        }

        return hiveTaskList;
    }

    private String getHeader(String dbAndTableName) {
        StringBuilder headerBuilder = new StringBuilder();
        try {
            String tableName = dbAndTableName.split("[.]")[1];
            if (tableName == null)
                return null;

            List<String> headerList = metadbService.findEngColumnNames(tableName);
            for (int i = 0; i < headerList.size(); i++) {
                headerBuilder.append(headerList.get(i));
                if (i < headerList.size() - 1)
                    headerBuilder.append(',');
            }
        } catch (Exception e) {
            return null;
        }
        return headerBuilder.toString();
    }

    private String buildWhereClause(List<String> columnNameList, Map<String/*column*/, List<String>/*values*/> conditionMap) {
        StringBuilder hiveWhereClauseBuilder = new StringBuilder();
        hiveWhereClauseBuilder.append(" WHERE ");

        for (int columnIndex = 0; columnIndex < columnNameList.size(); columnIndex++) {
            String columnName = columnNameList.get(columnIndex);
            List<String> values = conditionMap.get(columnName);

            if (values == null || values.size() == 0)
                return null;

            if (values.size() == 1) {
                hiveWhereClauseBuilder.append(getEquality(columnName, values.get(0)));
            } else {
                hiveWhereClauseBuilder.append('(');
                for (int valueIndex = 0; valueIndex < values.size(); valueIndex++) {
                    hiveWhereClauseBuilder.append(getEquality(columnName, values.get(valueIndex)));

                    if (valueIndex < values.size() - 1)
                        hiveWhereClauseBuilder.append(" OR ");
                }
                hiveWhereClauseBuilder.append(')');
            }

            if (columnIndex < columnNameList.size() - 1)
                hiveWhereClauseBuilder.append(" AND ");
        }
        return hiveWhereClauseBuilder.toString();
    }

    private String getEquality(String columnName, String value) {
        if (DataProcessorUtil.isNumeric(value))
            return String.format("%s = %s", columnName, value);

        return String.format("%s = '%s'", columnName, value);
    }
}