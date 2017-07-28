package org.bigdatacenter.dataprocessor.platform.resolver.query.join;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.key.ParameterMapKey;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.resolver.query.common.HiveQueryUtil;
import org.bigdatacenter.dataprocessor.platform.resolver.query.join.builder.HiveJoinQueryBuilder;
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
                                                                Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap
    ) {
        List<HiveTask> hiveTaskList = null;

        //
        // TODO: 다음 3개의 CASE 중 수행할 작업을 판별한다.
        //
        final RequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Map<Integer/*Year*/, Map<String/*Column Name*/, List<String/*Table Name*/>>> columnKeyMap = getColumnKeyMap(extractionParameter.getParameterMap());

        try {
            for (Integer year : columnKeyMap.keySet()) {
                final Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMapValue = columnKeyMap.get(year);
                final List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(year);
                final Integer joinTaskType = getJoinTaskType(columnKeyMapValue);

                switch (joinTaskType) {
                    case EXCLUSIVE_COLUMN_ZERO: // case 1: 모든 테이블에 있는 컬럼만 필터링 (No Join)
                        logger.info(String.format("%s - Join Task is EXCLUSIVE_COLUMN_ZERO", currentThreadName));
                        break;
                    case EXCLUSIVE_COLUMN_ONE: // case 2: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (1개) 필터링
                        logger.info(String.format("%s - Join Task is EXCLUSIVE_COLUMN_ONE", currentThreadName));
                        hiveTaskList = new ArrayList<>(hiveJoinQueryBuilder.buildHiveJoinQueryTasks(
                                joinTaskType, requestInfo, hiveJoinParameterList, columnKeyMapValue));
                        break;
                    case EXCLUSIVE_COLUMN_TWO_OR_MORE: // case 3: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (2개 이상) 필터링
                        logger.info(String.format("%s - Join Task is EXCLUSIVE_COLUMN_TWO_OR_MORE", currentThreadName));
                        hiveTaskList = new ArrayList<>(hiveJoinQueryBuilder.buildHiveJoinQueryTasks(
                                joinTaskType, requestInfo, hiveJoinParameterList, columnKeyMapValue));
                        break;
                    default:
                        logger.error(String.format("%s - Invalid join operation option", currentThreadName));
                        throw new NullPointerException();
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - Exception occurs at buildHiveJoinTasksWithExtractionTasks: %s", currentThreadName, e.getMessage()));
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        return hiveTaskList;
    }

    @Override
    public Map<Integer/*Year*/, Map<String/*Column Name*/, List<String/*Table Name*/>>> getColumnKeyMap(Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> parameterMap) {
        Map<Integer/*Year*/, Map<String/*Column Name*/, List<String/*Table Name*/>>> columnKeyMap = new HashMap<>();

        for (ParameterMapKey parameterMapKey : parameterMap.keySet()) {
            final Map<String/*column*/, List<String>/*values*/> parameterMapValue = parameterMap.get(parameterMapKey);
            final String dbAndTableName = HiveQueryUtil.getDbAndTableNameForQuery(parameterMapKey.getDbName(), parameterMapKey.getTableName());
            final Integer year = parameterMapKey.getYear();

            if (parameterMapValue != null) {
                for (String columnName : parameterMapValue.keySet()) {
                    Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMapValue = columnKeyMap.get(year);
                    List<String> tableNameList;

                    if (columnKeyMapValue == null) {
                        columnKeyMapValue = new HashMap<>();
                        tableNameList = new ArrayList<>();

                        tableNameList.add(dbAndTableName);
                        columnKeyMapValue.put(columnName, tableNameList);
                        columnKeyMap.put(year, columnKeyMapValue);
                    } else {
                        tableNameList = columnKeyMapValue.get(columnName);

                        //noinspection Duplicates
                        if (tableNameList == null) {
                            tableNameList = new ArrayList<>();

                            tableNameList.add(dbAndTableName);
                            columnKeyMapValue.put(columnName, tableNameList);
                        } else {
                            tableNameList.add(dbAndTableName);
                        }
                    }
                }
            }
        }

        return columnKeyMap;
    }

    @Override
    public Integer getJoinTaskType(Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap) {
        Integer joinTaskType;
        Map<String/*Table name*/, Boolean> validationMap = new HashMap<>();

        for (String columnName : columnKeyMap.keySet()) {
            List<String> tableNameList = columnKeyMap.get(columnName);
            if (tableNameList.size() == 1) {
                String exclusiveTableName = tableNameList.get(0);
                if (validationMap.get(exclusiveTableName) == null)
                    validationMap.put(exclusiveTableName, Boolean.TRUE);
            }
        }

        switch (validationMap.size()) {
            case 0:
                joinTaskType = HiveJoinQueryResolver.EXCLUSIVE_COLUMN_ZERO;
                break;
            case 1:
                joinTaskType = HiveJoinQueryResolver.EXCLUSIVE_COLUMN_ONE;
                break;
            default:
                joinTaskType = HiveJoinQueryResolver.EXCLUSIVE_COLUMN_TWO_OR_MORE;
        }

        return joinTaskType;
    }

    @Override
    public HiveTask buildHiveJoinTaskWithOutExtractionTask(HiveJoinParameter hiveJoinParameter,
                                                           HiveCreationTask hiveCreationTask, Integer year,
                                                           Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap
    ) {
        try {
            List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(year);
            //noinspection Duplicates
            if (hiveJoinParameterList == null) {
                hiveJoinParameterList = new ArrayList<>();
                hiveJoinParameterList.add(hiveJoinParameter);
                hiveJoinParameterListMap.put(year, hiveJoinParameterList);
            } else {
                hiveJoinParameterList.add(hiveJoinParameter);
            }

            if (hiveJoinParameter.getIsCreatable())
                return new HiveTask(hiveCreationTask, null);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - Exception occurs at buildHiveJoinTaskWithOutExtractionTask: %s", currentThreadName, e.getMessage()));
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        return null;
    }
}