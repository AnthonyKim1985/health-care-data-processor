package org.bigdatacenter.dataprocessor.platform.resolver.query;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.parameter.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;
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
        if (requestInfo == null) {
            logger.error(String.format("%s - RequestInfo not found", currentThreadName));
            return null;
        }

        // TODO: find request year
        List<RequestYearInfo> requestYearInfoList = metadbService.findRequestYears(dataSetUID);
        if (requestYearInfoList == null) {
            logger.error(String.format("%s - RequestYearInfo not found", currentThreadName));
            return null;
        }

        // TODO: find request filters
        List<RequestFilterInfo> requestFilterInfoList = metadbService.findRequestFilters(dataSetUID);
        if (requestFilterInfoList == null) {
            logger.error(String.format("%s - FilterInfo not found", currentThreadName));
            return null;
        }

        // TODO: find request indicator
        String indicator = takeIndicatorTask(dataSetUID);

        // TODO: find database
        MetaDatabaseInfo metaDatabaseInfo = metadbService.findMetaDatabase(requestInfo.getDatasetID());
        if (metaDatabaseInfo == null) {
            logger.error(String.format("%s - Meta Database not found", currentThreadName));
            return null;
        }

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
                    if (filterValues == null) {
                        logger.error(String.format("%s - FilterValue is null", currentThreadName));
                        return null;
                    }

                    for (String value : filterValues.split("[,]"))
                        taskInfoList.add(new TaskInfo(metaDatabaseInfo.getEdl_eng_name(), metaTableInfo.getEtl_eng_name(), metaColumnInfo.getEcl_eng_name(), value));
                }
            }
        }

        return new ExtractionParameter(requestInfo, indicator, convertTaskInfoListToParameterMap(taskInfoList));
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        final RequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = extractionParameter.getParameterMap();

        final Integer dataSetUID = requestInfo.getDataSetUID();
        final String indicatorHeader = extractionParameter.getIndicator();

        final List<HiveTask> hiveTaskList = new ArrayList<>();
        final Map<String, List<HiveJoinParameter>> hiveJoinParameterListMap = new HashMap<>();

        for (String dbAndTableName : parameterMap.keySet()) {
            if (dbAndTableName.contains("ykiho")) {
                logger.warn(String.format("%s - ykiho has been skipped", currentThreadName));
                continue;
            }

            StringBuilder hiveQueryBuilder = new StringBuilder();
            final String header = (indicatorHeader == null ? getHeader(dbAndTableName) : indicatorHeader);
            if (header == null) {
                logger.error(String.format("%s - header is null at buildExtractionRequest", currentThreadName));
                return null;
            }

            logger.debug(String.format("%s - header in buildExtractionRequest: %s", currentThreadName, header));
            hiveQueryBuilder.append(String.format("SELECT %s FROM %s", header, dbAndTableName));

            Map<String/*column*/, List<String>/*values*/> conditionMap = parameterMap.get(dbAndTableName);
            List<String> columnNameList = new ArrayList<>();
            columnNameList.addAll(conditionMap.keySet());

            if (columnNameList.size() > 0)
                hiveQueryBuilder.append(buildWhereClause(columnNameList, conditionMap));

            try {
                hiveTaskList.add(buildHiveTask(hiveJoinParameterListMap, hiveQueryBuilder.toString(), dbAndTableName, dataSetUID, requestInfo.getJoinCondition(), header));
            } catch (ArrayIndexOutOfBoundsException | NullPointerException e) {
                logger.error(String.format("%s - During the building hive task, exception occurs: hiveTask is null.", currentThreadName));
                return null;
            }
        }

        try {
            hiveTaskList.addAll(buildHiveJoinTasks(hiveJoinParameterListMap, requestInfo.getJoinCondition(), dataSetUID));
        } catch (ArrayIndexOutOfBoundsException | NullPointerException e) {
            logger.error(String.format("%s - During the building hive join tasks, exception occurs: hiveJoinTasks are null.", currentThreadName));
            return null;
        }

        return new ExtractionRequest(requestInfo, extractionParameter.getIndicator(), hiveTaskList);
    }

    private HiveTask buildHiveTask(Map<String, List<HiveJoinParameter>> hiveJoinParameterListMap, String hiveQuery, String dbAndTableName, Integer dataSetUID, Integer joinCondition, String header) {
        HiveTask hiveTask;
        try {
            final String dbName = dbAndTableName.split("[.]")[0];
            final String tableName = dbAndTableName.split("[.]")[1];

            final String hashedDbAndTableName = String.format("%s_extracted.%s_%s", dbName, tableName, DataProcessorUtil.getHashedString(hiveQuery)); // hashed value for hiveQuery
            final HiveCreationTask hiveCreationTask = new HiveCreationTask(hashedDbAndTableName, hiveQuery);

            switch (joinCondition) {
                case 0: // No Join Query
                    // /tmp/health_care/{dbAndTableName}/{dataSetUID}/{timeStamp}
                    final String hdfsLocation = String.format("/tmp/health_care/%s/%d/%s", dbAndTableName,
                            dataSetUID, String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
                    final HiveExtractionTask hiveExtractionTask = new HiveExtractionTask(hdfsLocation, String.format("SELECT * FROM %s", hashedDbAndTableName), header);
                    hiveTask = new HiveTask(hiveCreationTask, hiveExtractionTask);
                    break;
                case 1: // Join Query with KEY_SEQ
                case 2: // Join Query with PERSON_ID
                    final String tableNameSplitted[] = tableName.split("[_]");
                    final String mapKey = tableNameSplitted[tableNameSplitted.length - 1];

                    List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(mapKey);
                    if (hiveJoinParameterList == null) {
                        hiveJoinParameterList = new ArrayList<>();
                        hiveJoinParameterList.add(new HiveJoinParameter(dbName, tableName, hashedDbAndTableName, header));
                        hiveJoinParameterListMap.put(mapKey, hiveJoinParameterList);
                    } else {
                        hiveJoinParameterList.add(new HiveJoinParameter(dbName, tableName, hashedDbAndTableName, header));
                    }
                    hiveTask = new HiveTask(hiveCreationTask, null);
                    break;
                default:
                    logger.error(String.format("%s - Invalid join condition: %d", currentThreadName, joinCondition));
                    throw new NullPointerException();
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - split exception occurs at dbAndTableName", currentThreadName));
            throw new ArrayIndexOutOfBoundsException();
        }
        return hiveTask;
    }

    private List<HiveTask> buildHiveJoinTasks(Map<String, List<HiveJoinParameter>> hiveJoinParameterListMap, Integer joinCondition, Integer dataSetUID) {
        List<HiveTask> hiveTaskList = new ArrayList<>();

        for (String key : hiveJoinParameterListMap.keySet()) {
            List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(key);
            try {
                switch (joinCondition) {
                    case 1: // Take join operation by KEY_SEQ
                        hiveTaskList.addAll(getHiveJoinTasks(hiveJoinParameterList, "key_seq", dataSetUID));
                        break;
                    case 2: // Take join operation by PERSON_ID
                        hiveTaskList.addAll(getHiveJoinTasks(hiveJoinParameterList, "person_id", dataSetUID));
                        break;
                    default:
                        logger.error(String.format("%s - Invalid join condition: %d", currentThreadName, joinCondition));
                        throw new NullPointerException();
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.error(String.format("%s - split exception occurs at getHiveJoinTasks: %s", currentThreadName, e.getMessage()));
                throw new ArrayIndexOutOfBoundsException();
            }
        }
        return hiveTaskList;
    }

    private List<HiveTask> getHiveJoinTasks(List<HiveJoinParameter> hiveJoinParameterList, String joinKey, Integer dataSetUID) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        for (int i = 0; i < hiveJoinParameterList.size(); i++) {
            HiveJoinParameter hiveJoinParameter = hiveJoinParameterList.get(i);

            final String hiveJoinQuery = getHiveJoinQuery(sortParameterList(hiveJoinParameterList, i), joinKey);
            final String hashedTableName = String.format("%s_%s", hiveJoinParameter.getTableName(), DataProcessorUtil.getHashedString(hiveJoinQuery));
            final String integratedHashedDbAndTableName = String.format("%s_join_%s_integrated.%s", hiveJoinParameter.getDbName(), joinKey, hashedTableName);

            logger.info(String.format("%s - HiveJoinQuery: %s", currentThreadName, hiveJoinQuery));
            logger.info(String.format("%s - HashedTableName: %s", currentThreadName, hashedTableName));

            // /tmp/health_care/{dbAndTableName}/{dataSetUID}/{timeStamp}
            final String hdfsLocation = String.format("/tmp/health_care/%s.%s/%d/%s", hiveJoinParameter.getDbName(), hiveJoinParameter.getTableName(),
                    dataSetUID, String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
            final HiveCreationTask hiveCreationTask = new HiveCreationTask(integratedHashedDbAndTableName, hiveJoinQuery);
            final HiveExtractionTask hiveExtractionTask = new HiveExtractionTask(hdfsLocation, String.format("SELECT * FROM %s", integratedHashedDbAndTableName), hiveJoinParameter.getHeader());

            hiveTaskList.add(new HiveTask(hiveCreationTask, hiveExtractionTask));
        }

        return hiveTaskList;
    }

    private List<HiveJoinParameter> sortParameterList(List<HiveJoinParameter> hiveJoinParameterList, int entryIndex) {
        final List<HiveJoinParameter> sortedHiveJoinParameterList = new ArrayList<>();

        sortedHiveJoinParameterList.add(hiveJoinParameterList.get(entryIndex));

        for (int i = 0; i < hiveJoinParameterList.size(); i++) {
            if (i == entryIndex) continue;
            HiveJoinParameter hiveJoinParameter = hiveJoinParameterList.get(i);
            sortedHiveJoinParameterList.add(hiveJoinParameter);
        }
        return sortedHiveJoinParameterList;
    }

    private String getHiveJoinQuery(List<HiveJoinParameter> hiveJoinParameterList, String joinKey) {
        final StringBuilder joinQueryBuilder = new StringBuilder();
        final String entryAlias = hiveJoinParameterList.get(0).getHashedDbAndTableName().split("[.]")[1].split("[_]")[3];
        joinQueryBuilder.append(String.format("SELECT DISTINCT %s.* FROM %s %s", entryAlias, hiveJoinParameterList.get(0).getHashedDbAndTableName(), entryAlias));

        for (int i = 0; i < hiveJoinParameterList.size() - 1; i++) {
            final String currentAlias = hiveJoinParameterList.get(i).getHashedDbAndTableName().split("[.]")[1].split("[_]")[3];
            final String nextAlias = hiveJoinParameterList.get(i + 1).getHashedDbAndTableName().split("[.]")[1].split("[_]")[3];

            joinQueryBuilder.append(String.format(" INNER JOIN %s %s ON (%s.%s = %s.%s)", hiveJoinParameterList.get(i + 1).getHashedDbAndTableName(), nextAlias, currentAlias, joinKey, nextAlias, joinKey));
        }

        return joinQueryBuilder.toString();
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

    private String getHeader(String dbAndTableName) {
        StringBuilder headerBuilder = new StringBuilder();
        try {
            String tableName = dbAndTableName.split("[.]")[1];
            if (tableName == null)
                return null;

            List<String> headerList = metadbService.findEngColumnNames(tableName);
            for (int i = 0; i < headerList.size(); i++) {
                headerBuilder.append(headerList.get(i).trim());
                if (i < headerList.size() - 1)
                    headerBuilder.append(',');
            }
        } catch (Exception e) {
            logger.warn(String.format("%s - invalid dbAndTableName: %s", currentThreadName, dbAndTableName));
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