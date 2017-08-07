package org.bigdatacenter.dataprocessor.platform.resolver.query_old;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ParameterMapKey;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.request.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query_old.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaDatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaSelectedColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.meta.MetaTableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestYearInfo;
import org.bigdatacenter.dataprocessor.platform.resolver.query_old.common.HiveQueryUtil;
import org.bigdatacenter.dataprocessor.platform.resolver.query_old.join.HiveJoinQueryResolver;
import org.bigdatacenter.dataprocessor.platform.service.metadb.MetadbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
    private static final String currentThreadName = Thread.currentThread().getName();

    @Autowired
    private MetadbService metadbService;

    @Autowired
    private HiveJoinQueryResolver hiveJoinQueryResolver;

    @Override
    public ExtractionParameter buildExtractionParameter(Integer dataSetUID) {
        final List<TaskInfo> taskInfoList = new ArrayList<>();

        try {
            // TODO: find request
            final RequestInfo requestInfo = metadbService.findRequest(dataSetUID);
            if (requestInfo == null)
                throw new NullPointerException(String.format("%s - RequestInfo not found", currentThreadName));

            // TODO: find request year
            final List<RequestYearInfo> requestYearInfoList = metadbService.findRequestYears(dataSetUID);
            if (requestYearInfoList == null)
                throw new NullPointerException(String.format("%s - RequestYearInfo not found", currentThreadName));

            // TODO: find request filters
            final List<RequestFilterInfo> requestFilterInfoList = metadbService.findRequestFilters(dataSetUID);
            if (requestFilterInfoList == null)
                throw new NullPointerException(String.format("%s - FilterInfo not found", currentThreadName));

            // TODO: find database
            final MetaDatabaseInfo metaDatabaseInfo = metadbService.findMetaDatabase(requestInfo.getDatasetID());
            if (metaDatabaseInfo == null)
                throw new NullPointerException(String.format("%s - Meta Database not found", currentThreadName));

            // TODO: make tasks
            for (RequestYearInfo requestYearInfo : requestYearInfoList) {
                for (RequestFilterInfo requestFilterInfo : requestFilterInfoList) {
                    // TODO: find column
                    final Integer year = Integer.parseInt(requestYearInfo.getYearName());
                    final List<MetaColumnInfo> metaColumnInfoList = metadbService.findMetaColumns(
                            requestInfo.getDatasetID(), requestFilterInfo.getFilterEngName(), year);

                    logger.debug(String.format("%s - %s", currentThreadName, metaColumnInfoList));

                    for (MetaColumnInfo metaColumnInfo : metaColumnInfoList) {
                        final MetaTableInfo metaTableInfo = metadbService.findMetaTable(metaColumnInfo.getEtl_idx());
                        if (metaTableInfo == null) {
                            logger.warn(String.format("%s - The meta information for the table could not be found. (etl_idx: %d)", currentThreadName, metaColumnInfo.getEtl_idx()));
                            continue;
                        }

                        String filterValues = requestFilterInfo.getFilterValues();
                        if (filterValues == null)
                            throw new NullPointerException(String.format("%s - FilterValue is null", currentThreadName));

                        for (String value : filterValues.split("[,]"))
                            taskInfoList.add(new TaskInfo(metaDatabaseInfo.getEdl_eng_name(), metaTableInfo.getEtl_eng_name(), year, metaColumnInfo.getEcl_eng_name(), value));
                    }
                }
            }

            return new ExtractionParameter(requestInfo, convertTaskInfoListToParameterMap(requestInfo, metaDatabaseInfo, requestYearInfoList, taskInfoList));
        } catch (Exception e) {
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        if (extractionParameter == null)
            throw new NullPointerException(String.format("%s - extractionParameter is null.", currentThreadName));

        try {
            final RequestInfo requestInfo = extractionParameter.getRequestInfo();
            final Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> parameterMap = extractionParameter.getParameterMap();

            final List<HiveTask> hiveTaskList = new ArrayList<>();
            final Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap = new HashMap<>();
            final Map<Integer/*Year*/, List<HiveTask>> hiveTaskListMapForExtractionTask = new HashMap<>();

            for (ParameterMapKey parameterMapKey : parameterMap.keySet()) {
                final String dbName = parameterMapKey.getDbName();
                final String tableName = parameterMapKey.getTableName();
                final String dbAndTableName = HiveQueryUtil.concatDbAndTableName(dbName, tableName);

                if (dbAndTableName.contains("ykiho")) {
                    logger.warn(String.format("%s - ykiho has been skipped", currentThreadName));
                    continue;
                }

                final StringBuilder hiveQueryBuilder = new StringBuilder();
                hiveQueryBuilder.append(HiveQueryUtil.getSelectAllQuery(dbAndTableName));

                Boolean isCreatable = Boolean.FALSE;
                Map<String/*column*/, List<String>/*values*/> conditionMap = parameterMap.get(parameterMapKey);

                if (conditionMap != null)
                    if (conditionMap.size() > 0) {
                        List<String> columnNameList = new ArrayList<>();
                        columnNameList.addAll(conditionMap.keySet());
                        hiveQueryBuilder.append(buildWhereClause(columnNameList, conditionMap));

                        isCreatable = Boolean.TRUE;
                    }

                HiveTask hiveTask = buildHiveTask(extractionParameter, hiveTaskListMapForExtractionTask,
                        hiveJoinParameterListMap, parameterMapKey, hiveQueryBuilder.toString(), isCreatable);

                if (hiveTask != null)
                    hiveTaskList.add(hiveTask);
            }

            switch (requestInfo.getJoinCondition()) {
                case 1: // Take join operation by KEY_SEQ
                case 2: // Take join operation by PERSON_ID
                    hiveTaskList.addAll(hiveJoinQueryResolver.buildHiveJoinTasksWithExtractionTasks(
                            extractionParameter, hiveJoinParameterListMap, hiveTaskListMapForExtractionTask));
                    break;
            }

            return new ExtractionRequest(requestInfo, hiveTaskList);
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }
    }

    private HiveTask buildHiveTask(ExtractionParameter extractionParameter,
                                   Map<Integer/*Year*/, List<HiveTask>> hiveTaskListMapForExtractionTask,
                                   Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap,
                                   ParameterMapKey parameterMapKey,
                                   String hiveQuery, Boolean isCreatable
    ) {
        final RequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Integer dataSetUID = requestInfo.getDataSetUID();
        final Integer joinCondition = requestInfo.getJoinCondition();

        HiveTask hiveTask;

        final Integer year = parameterMapKey.getYear();
        final String dbName = parameterMapKey.getDbName();
        final String tableName = parameterMapKey.getTableName();
        final String header = getHiveTableHeader(tableName, dataSetUID); // table header to be extracted
        final String dbAndTableName = HiveQueryUtil.concatDbAndTableName(dbName, tableName);

        String dbAndHashedTableName = null;
        HiveCreationTask hiveCreationTask = null;
        HiveExtractionTask hiveExtractionTask = null;

        if (isCreatable) {
            dbAndHashedTableName = HiveQueryUtil.getDbAndTableNameForExtractedDataSet(dbName, tableName, hiveQuery);
            hiveCreationTask = new HiveCreationTask(dbAndHashedTableName, hiveQuery);
            hiveExtractionTask = new HiveExtractionTask(
                    DataProcessorUtil.getHdfsLocation(dbAndTableName, dataSetUID),
                    HiveQueryUtil.getSelectAllQuery(dbAndHashedTableName), header);
        }

        switch (joinCondition) {
            case 0: // No Join Query
                hiveTask = new HiveTask(hiveCreationTask, hiveExtractionTask);
                break;
            case 1: // Join Query with KEY_SEQ
            case 2: // Join Query with PERSON_ID
                if (isCreatable) {
                    List<HiveTask> hiveTaskListForExtractionTask = hiveTaskListMapForExtractionTask.get(year);
                    if (hiveTaskListForExtractionTask == null) {
                        hiveTaskListForExtractionTask = new ArrayList<>();

                        hiveTaskListForExtractionTask.add(new HiveTask(null, hiveExtractionTask));
                        hiveTaskListMapForExtractionTask.put(year, hiveTaskListForExtractionTask);
                    } else {
                        hiveTaskListForExtractionTask.add(new HiveTask(null, hiveExtractionTask));
                    }
                }

                HiveJoinParameter hiveJoinParameter = new HiveJoinParameter(
                        dbName, tableName, isCreatable ? dbAndHashedTableName : dbAndTableName, header, isCreatable);

                hiveTask = hiveJoinQueryResolver.buildHiveJoinTaskWithOutExtractionTask(
                        hiveJoinParameter, hiveCreationTask, parameterMapKey.getYear(), hiveJoinParameterListMap);
                break;
            default:
                throw new NullPointerException(String.format("%s - Invalid join condition: %d", currentThreadName, joinCondition));
        }

        return hiveTask;
    }

    private Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> convertTaskInfoListToParameterMap(RequestInfo requestInfo,
                                                                                                                  MetaDatabaseInfo metaDatabaseInfo,
                                                                                                                  List<RequestYearInfo> requestYearInfoList,
                                                                                                                  List<TaskInfo> taskInfoList
    ) {
        //
        // TODO: Fill parameterMap Keys with all table name of the db
        //
        final Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> parameterMap;
        try {
            parameterMap = getParameterMapFilledWithAllTableNameForDb(requestInfo, metaDatabaseInfo, requestYearInfoList);
        } catch (NullPointerException e) {
            throw new NullPointerException(e.getMessage());
        }

        //
        // TODO: Process taskInfoList
        //
        for (TaskInfo taskInfo : taskInfoList) {
            ParameterMapKey parameterMapKey = new ParameterMapKey(taskInfo.getDatabaseName(), taskInfo.getTableName(), taskInfo.getYear());

            if (parameterMapKey.getTableName().contains("ykiho")) {
                logger.warn(String.format("%s - ykiho has been skipped", currentThreadName));
                continue;
            }

            Map<String/*column*/, List<String>/*values*/> parameterMapValue = parameterMap.get(parameterMapKey);

            List<String> values;
            if (parameterMapValue == null) {
                parameterMapValue = new HashMap<>();
                values = new ArrayList<>();

                values.add(taskInfo.getValue());
                parameterMapValue.put(taskInfo.getColumnName(), values);
                parameterMap.put(parameterMapKey, parameterMapValue);
            } else {
                values = parameterMapValue.get(taskInfo.getColumnName());

                if (values == null) {
                    values = new ArrayList<>();

                    values.add(taskInfo.getValue());
                    parameterMapValue.put(taskInfo.getColumnName(), values);
                } else {
                    values.add(taskInfo.getValue());
                }
            }
        }

        return parameterMap;
    }

    private Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> getParameterMapFilledWithAllTableNameForDb(RequestInfo requestInfo,
                                                                                                                           MetaDatabaseInfo metaDatabaseInfo,
                                                                                                                           List<RequestYearInfo> requestYearInfoList
    ) {
        final Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> parameterMap = new HashMap<>();

        final Integer joinCondition = requestInfo.getJoinCondition();
        switch (joinCondition) {
            case 0:
                logger.info(String.format("%s - Skip filling parameterMap Keys with all table name of the db. (Join Condition: %d)", currentThreadName, joinCondition));
                break;
            case 1:
            case 2:
                for (RequestYearInfo requestYearInfo : requestYearInfoList) {
                    List<String> foundMetaTableNames = metadbService.findMetaTableNames(metaDatabaseInfo.getEdl_idx(), Integer.parseInt(requestYearInfo.getYearName()));
                    if (foundMetaTableNames == null)
                        throw new NullPointerException("Could not find any table name. Please check meta database.");

                    for (String foundMetaTableName : foundMetaTableNames)
                        if (!foundMetaTableName.contains("ykiho"))
                            parameterMap.put(new ParameterMapKey(metaDatabaseInfo.getEdl_eng_name(), foundMetaTableName, Integer.parseInt(requestYearInfo.getYearName())), null);
                }

                logger.info(String.format("%s - Fill parameterMap Keys with all table name of the db. (Join Condition: %d)", currentThreadName, joinCondition));
                break;
            default:
                throw new NullPointerException(String.format("%s - Invalid Join Condition: %d (valid value: 0 to 2)", currentThreadName, joinCondition));
        }
        return parameterMap;
    }

    private String getHiveTableHeader(String tableName, Integer dataSetUID) {
        final StringBuilder headerBuilder = new StringBuilder();

        final List<MetaSelectedColumnInfo> metaSelectedColumnInfoList = metadbService.findMetaSelectedColumns(dataSetUID, tableName);
        if (metaSelectedColumnInfoList == null || metaSelectedColumnInfoList.isEmpty()) {
            final List<String> columnNameList = metadbService.findEngColumnNames(tableName);
            if (columnNameList == null || columnNameList.isEmpty())
                throw new NullPointerException(String.format("%s - The column list meta data for tableName %s not exists.", currentThreadName, tableName));

            final Integer columnNameListSize = columnNameList.size();
            for (int i = 0; i < columnNameListSize; i++) {
                headerBuilder.append(columnNameList.get(i).trim());
                if (i < columnNameListSize - 1)
                    headerBuilder.append(',');
            }
        } else {
            final Integer metaSelectedColumnInfoListSize = metaSelectedColumnInfoList.size();
            for (int i = 0; i < metaSelectedColumnInfoListSize; i++) {
                headerBuilder.append(metaSelectedColumnInfoList.get(i).getEcl_eng_name());
                if (i < metaSelectedColumnInfoListSize - 1)
                    headerBuilder.append(',');
            }
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
                throw new NullPointerException(String.format("%s - Any values are not found at buildWhereClause. (key: %s)", currentThreadName, columnName));

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