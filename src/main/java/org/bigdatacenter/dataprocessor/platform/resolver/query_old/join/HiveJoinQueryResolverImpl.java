package org.bigdatacenter.dataprocessor.platform.resolver.query_old.join;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ParameterMapKey;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query_old.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query_old.join.map.key.column.value.ColumnKeyMapValue;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.resolver.query_old.join.builder.HiveJoinQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-17.
 */
@Component
public class HiveJoinQueryResolverImpl implements HiveJoinQueryResolver {
    private static final Logger logger = LoggerFactory.getLogger(HiveJoinQueryResolverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    @Autowired
    private HiveJoinQueryBuilder hiveJoinQueryBuilder;

    @Override
    public List<HiveTask> buildHiveJoinTasksWithExtractionTasks(ExtractionParameter extractionParameter,
                                                                Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap,
                                                                Map<Integer/*Year*/, List<HiveTask>> hiveTaskListMapForExtractionTask
    ) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        //
        // TODO: 다음 3개의 CASE 중 수행할 작업을 판별한다.
        //
        final RequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Map<Integer/*Year*/, Map<String/*Column Name*/, List<ColumnKeyMapValue>>> yearKeyMap = getYearKeyMap(extractionParameter.getParameterMap());

        logger.debug(String.format("%s - columnKeyMap: %s", currentThreadName, yearKeyMap));

        switch (requestInfo.getJoinCondition()) {
            case 1:
                logger.info(String.format("%s - Take join operation by KEY_SEQ", currentThreadName));
                break;
            case 2:
                logger.info(String.format("%s - Take join operation by PERSON_ID", currentThreadName));
                break;
            default:
                logger.error(String.format("%s - Invalid join operation option", currentThreadName));
                throw new NullPointerException();
        }

        try {
            for (Integer year : yearKeyMap.keySet()) {
                final Map<String/*Column Name*/, List<ColumnKeyMapValue>> yearKeyMapValue = yearKeyMap.get(year);
                final List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(year);
                final Integer joinTaskType = getJoinTaskType(yearKeyMapValue);

                logger.debug(String.format("%s - hiveJoinParameterList: %s", currentThreadName, hiveJoinParameterList));
                logger.debug(String.format("%s - joinTaskType: %d", currentThreadName, joinTaskType));

                switch (joinTaskType) {
                    case NON_EXCLUSIVE_COLUMN: // case 1: 모든 테이블에 있는 컬럼만 필터링 (No Join)
                        logger.info(String.format("%s - Join Task is NON_EXCLUSIVE_COLUMN", currentThreadName));
                        hiveTaskList.addAll(hiveTaskListMapForExtractionTask.get(year));
                        break;
                    case ONE_EXCLUSIVE_COLUMN: // case 2: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (1개) 필터링
                        logger.info(String.format("%s - Join Task is ONE_EXCLUSIVE_COLUMN", currentThreadName));
                        hiveTaskList.addAll(hiveJoinQueryBuilder.buildHiveJoinQueryTasks(
                                joinTaskType, requestInfo, hiveJoinParameterList, yearKeyMapValue));
                        break;
                    case TWO_OR_MORE_EXCLUSIVE_COLUMNS: // case 3: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (2개 이상) 필터링
                        logger.info(String.format("%s - Join Task is TWO_OR_MORE_EXCLUSIVE_COLUMNS", currentThreadName));
                        hiveTaskList.addAll(hiveJoinQueryBuilder.buildHiveJoinQueryTasks(
                                joinTaskType, requestInfo, hiveJoinParameterList, yearKeyMapValue));
                        break;
                    default:
                        logger.error(String.format("%s - Invalid join operation option", currentThreadName));
                        throw new NullPointerException();
                }
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw new NullPointerException(String.format("%s - Exception occurs at buildHiveJoinTasksWithExtractionTasks: %s", currentThreadName, e.getMessage()));
        }

        return hiveTaskList;
    }

    @Override
    public Map<Integer/*Year*/, Map<String/*Column Name*/, List<ColumnKeyMapValue>>> getYearKeyMap(Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> parameterMap) {
        final Map<Integer/*Year*/, Map<String/*Column Name*/, List<ColumnKeyMapValue>>> yearKeyMap = new HashMap<>();

        for (ParameterMapKey parameterMapKey : parameterMap.keySet()) {
            final Map<String/*column*/, List<String>/*values*/> parameterMapValue = parameterMap.get(parameterMapKey);
            final String dbName = parameterMapKey.getDbName();
            final String tableName = parameterMapKey.getTableName();
            final Integer year = parameterMapKey.getYear();

            final ColumnKeyMapValue columnKeyMapValue = new ColumnKeyMapValue(dbName, tableName);

            if (parameterMapValue != null) {
                for (String columnName : parameterMapValue.keySet()) {
                    Map<String/*Column Name*/, List<ColumnKeyMapValue>> yearKeyMapValue = yearKeyMap.get(year);
                    List<ColumnKeyMapValue> columnKeyMapValueList;

                    if (yearKeyMapValue == null) {
                        yearKeyMapValue = new HashMap<>();
                        columnKeyMapValueList = new ArrayList<>();

                        columnKeyMapValueList.add(columnKeyMapValue);
                        yearKeyMapValue.put(columnName, columnKeyMapValueList);
                        yearKeyMap.put(year, yearKeyMapValue);
                    } else {
                        columnKeyMapValueList = yearKeyMapValue.get(columnName);

                        //noinspection Duplicates
                        if (columnKeyMapValueList == null) {
                            columnKeyMapValueList = new ArrayList<>();

                            columnKeyMapValueList.add(columnKeyMapValue);
                            yearKeyMapValue.put(columnName, columnKeyMapValueList);
                        } else {
                            columnKeyMapValueList.add(columnKeyMapValue);
                        }
                    }
                }
            }
        }

        return yearKeyMap;
    }

    @Override
    public Integer getJoinTaskType(Map<String/*Column Name*/, List<ColumnKeyMapValue>> columnKeyMap) {
        Integer joinTaskType;

        switch (findExclusiveColumns(columnKeyMap)) {
            case 0:
                joinTaskType = HiveJoinQueryResolver.NON_EXCLUSIVE_COLUMN;
                break;
            case 1:
                joinTaskType = HiveJoinQueryResolver.ONE_EXCLUSIVE_COLUMN;
                break;
            default:
                joinTaskType = HiveJoinQueryResolver.TWO_OR_MORE_EXCLUSIVE_COLUMNS;
        }

        return joinTaskType;
    }

    private Integer findExclusiveColumns(Map<String/*Column Name*/, List<ColumnKeyMapValue>> columnKeyMap) {
        Map<ColumnKeyMapValue, Boolean> validationMapForBeingExclusiveColumns = new HashMap<>();

        for (String columnName : columnKeyMap.keySet()) {
            List<ColumnKeyMapValue> columnKeyMapValueList = columnKeyMap.get(columnName);
            if (columnKeyMapValueList.size() == 1) {
                ColumnKeyMapValue exclusiveColumnKeyMapValue = columnKeyMapValueList.get(0);
                if (validationMapForBeingExclusiveColumns.get(exclusiveColumnKeyMapValue) == null)
                    validationMapForBeingExclusiveColumns.put(exclusiveColumnKeyMapValue, Boolean.TRUE);
            }
        }

        return validationMapForBeingExclusiveColumns.size();
    }

    @Override
    public HiveTask buildHiveJoinTaskWithOutExtractionTask(HiveJoinParameter hiveJoinParameter,
                                                           HiveCreationTask hiveCreationTask, Integer year,
                                                           Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap
    ) {
        List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(year);

        //noinspection Duplicates
        if (hiveJoinParameterList == null) {
            hiveJoinParameterList = new ArrayList<>();
            hiveJoinParameterList.add(hiveJoinParameter);
            hiveJoinParameterListMap.put(year, hiveJoinParameterList);
        } else {
            hiveJoinParameterList.add(hiveJoinParameter);
        }

        if (!hiveJoinParameter.getIsCreatable())
            return null;

        return new HiveTask(hiveCreationTask, null);
    }
}