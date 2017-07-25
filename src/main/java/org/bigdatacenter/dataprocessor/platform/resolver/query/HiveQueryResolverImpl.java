package org.bigdatacenter.dataprocessor.platform.resolver.query;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.parameter.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTaskAndExtractionTaskPair;
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
import org.bigdatacenter.dataprocessor.platform.resolver.query.join.HiveJoinQueryResolver;
import org.bigdatacenter.dataprocessor.platform.service.metadb.MetadbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
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
    private final String currentThreadName = Thread.currentThread().getName();

    @Autowired
    private MetadbService metadbService;

    @Autowired
    private HiveJoinQueryResolver hiveJoinQueryResolver;

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
                    if (metaTableInfo == null) {
                        logger.warn(String.format("%s - The meta information for the table could not be found. (etl_idx: %d)", currentThreadName, metaColumnInfo.getEtl_idx()));
                        continue;
                    }

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
        final List<HiveTask> hiveTaskList = new ArrayList<>();
        final Map<String/*dbAndTableName*/, HiveExtractionTask> hiveExtractionTaskMap = new HashMap<>();

        final String indicatorHeader = extractionParameter.getIndicator();

        for (String dbAndTableName : parameterMap.keySet()) {
            if (dbAndTableName.contains("ykiho")) {
                logger.warn(String.format("%s - ykiho has been skipped", currentThreadName));
                continue;
            }

            StringBuilder hiveQueryBuilder = new StringBuilder();
            final String header = (indicatorHeader == null ? getHiveTableHeader(dbAndTableName) : indicatorHeader);
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
                HiveTaskAndExtractionTaskPair hiveTaskAndExtractionTaskPair = buildHiveTask(extractionParameter, hiveQueryBuilder.toString(), dbAndTableName, header);
                hiveTaskList.add(hiveTaskAndExtractionTaskPair.getHiveTask());

                hiveExtractionTaskMap.put(hiveTaskAndExtractionTaskPair.getDbAndTableName(), hiveTaskAndExtractionTaskPair.getHiveExtractionTask());
            } catch (ArrayIndexOutOfBoundsException | NullPointerException e) {
                logger.error(String.format("%s - During the building hive task, exception occurs: hiveTask is null.", currentThreadName));
                e.printStackTrace();
                return null;
            }
        }

        if (requestInfo.getJoinCondition() > 0)
            try {
                hiveTaskList.addAll(hiveJoinQueryResolver.buildHiveJoinTasksWithExtractionTasks(extractionParameter, hiveExtractionTaskMap));
            } catch (ArrayIndexOutOfBoundsException | NullPointerException e) {
                logger.error(String.format("%s - During the building hive join tasks, exception occurs: hiveJoinTasks are null.", currentThreadName));
                e.printStackTrace();
                return null;
            }

        return new ExtractionRequest(requestInfo, extractionParameter.getIndicator(), hiveTaskList);
    }

    private HiveTaskAndExtractionTaskPair buildHiveTask(ExtractionParameter extractionParameter, String hiveQuery, String dbAndTableName, String header) {
        final RequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Integer dataSetUID = requestInfo.getDataSetUID();
        final Integer joinCondition = requestInfo.getJoinCondition();

        HiveTask hiveTask;
        try {
            final String splittedDbAndTableName[] = dbAndTableName.split("[.]");
            final String dbName = splittedDbAndTableName[0];
            final String tableName = splittedDbAndTableName[1];

            final String dbAndHashedTableName = String.format("%s_extracted.%s_%s", dbName, tableName, DataProcessorUtil.getHashedString(hiveQuery)); // hashed value for hiveQuery
            final HiveCreationTask hiveCreationTask = new HiveCreationTask(dbAndHashedTableName, hiveQuery);

            final String hdfsLocation = DataProcessorUtil.getHdfsLocation(dbAndTableName, dataSetUID);
            final HiveExtractionTask hiveExtractionTask = new HiveExtractionTask(hdfsLocation, String.format("SELECT * FROM %s", dbAndHashedTableName), header);

            switch (joinCondition) {
                case 0: // No Join Query
                    hiveTask = new HiveTask(hiveCreationTask, hiveExtractionTask);
                    break;
                case 1: // Join Query with KEY_SEQ
                case 2: // Join Query with PERSON_ID
                    HiveJoinParameter hiveJoinParameter = new HiveJoinParameter(dbName, tableName, dbAndHashedTableName, header);
                    hiveTask = hiveJoinQueryResolver.buildHiveJoinTaskWithOutExtractionTask(hiveJoinParameter, hiveCreationTask);
                    break;
                default:
                    logger.error(String.format("%s - Invalid join condition: %d", currentThreadName, joinCondition));
                    throw new NullPointerException();
            }
            return new HiveTaskAndExtractionTaskPair(hiveTask, dbAndTableName, hiveExtractionTask);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - split exception occurs at dbAndTableName", currentThreadName));
            throw new ArrayIndexOutOfBoundsException();
        }
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

        if (indicatorBuilder.toString().length() == 0) {
            logger.info(String.format("%s - The length of indicatorBuilder is 0 at takeIndicatorTask.", currentThreadName));
            return null;
        }

        return indicatorBuilder.toString();
    }

    private Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> convertTaskInfoListToParameterMap(List<TaskInfo> taskInfoList) {
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = new HashMap<>();

        for (TaskInfo taskInfo : taskInfoList) {
            String parameterKey = String.format("%s.%s", taskInfo.getDatabaseName(), taskInfo.getTableName());

            if (parameterKey.contains("ykiho")) {
                logger.warn(String.format("%s - ykiho has been skipped", currentThreadName));
                continue;
            }

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

    private String getHiveTableHeader(String dbAndTableName) {
        StringBuilder headerBuilder = new StringBuilder();
        try {
            String tableName = dbAndTableName.split("[.]")[1];
            if (tableName == null) {
                logger.error(String.format("%s - tableName is null at getHiveTableHeader.", currentThreadName));
                return null;
            }

            List<String> headerList = metadbService.findEngColumnNames(tableName);
            for (int i = 0; i < headerList.size(); i++) {
                headerBuilder.append(headerList.get(i).trim());
                if (i < headerList.size() - 1)
                    headerBuilder.append(',');
            }
        } catch (Exception e) {
            logger.error(String.format("%s - invalid dbAndTableName: %s", currentThreadName, dbAndTableName));
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

            if (values == null || values.size() == 0) {
                logger.error(String.format("%s - Any values are not found at buildWhereClause. (key: %s)", currentThreadName, columnName));
                return null;
            }

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