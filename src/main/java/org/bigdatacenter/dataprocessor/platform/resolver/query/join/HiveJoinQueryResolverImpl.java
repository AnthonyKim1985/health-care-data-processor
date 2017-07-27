package org.bigdatacenter.dataprocessor.platform.resolver.query.join;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.parameter.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
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
                                                                Map<String/*MapKey: Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap) {
        List<HiveTask> hiveTaskList = null;

        //
        // TODO: 다음 3개의 CASE 중 수행할 작업을 판별한다.
        //
        Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap = getColumnKeyMap(extractionParameter.getParameterMap());
        try {
            switch (getJoinTaskType(columnKeyMap)) {
                case EXCLUSIVE_COLUMN_ZERO: // case 1: 모든 테이블에 있는 컬럼만 필터링 (No Join)
                    logger.info(String.format("%s - Join Task is EXCLUSIVE_COLUMN_ZERO", currentThreadName));
                    break;
                case EXCLUSIVE_COLUMN_ONE: // case 2: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (1개) 필터링
                    logger.info(String.format("%s - Join Task is EXCLUSIVE_COLUMN_ONE", currentThreadName));
                    hiveTaskList = new ArrayList<>(hiveJoinQueryBuilder.buildHiveJoinQueryTasks(extractionParameter, columnKeyMap, hiveJoinParameterListMap));
                    break;
                case EXCLUSIVE_COLUMN_TWO_OR_MORE: // case 3: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (2개 이상) 필터링
                    logger.info(String.format("%s - Join Task is EXCLUSIVE_COLUMN_TWO_OR_MORE", currentThreadName));
                    hiveTaskList = new ArrayList<>(hiveJoinQueryBuilder.buildHiveJoinQueryTasks(extractionParameter, columnKeyMap, hiveJoinParameterListMap));
                    break;
                default:
                    logger.error(String.format("%s - Invalid join operation option", currentThreadName));
                    throw new NullPointerException();
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - Exception occurs at buildHiveJoinTasksWithExtractionTasks: %s", currentThreadName, e.getMessage()));
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        return hiveTaskList;
    }

    @Override
    public Map<String/*Column Name*/, List<String/*Table Name*/>> getColumnKeyMap(Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap) {
        Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap = new HashMap<>();

        for (String dbAndTableName : parameterMap.keySet()) {
            Map<String/*column*/, List<String>/*values*/> parameterMapValue = parameterMap.get(dbAndTableName);
            if (parameterMapValue != null)
                for (String columnName : parameterMapValue.keySet()) {
                    List<String> tableNameList = columnKeyMap.get(columnName);
                    //noinspection Duplicates
                    if (tableNameList == null) {
                        tableNameList = new ArrayList<>();

                        tableNameList.add(dbAndTableName);
                        columnKeyMap.put(columnName, tableNameList);
                    } else {
                        tableNameList.add(dbAndTableName);
                    }
                }
        }

        return columnKeyMap;
    }

    @Override
    public Integer getJoinTaskType(Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap) {
        int numberOfExclusiveColumnNames = 0;
        for (String columnName : columnKeyMap.keySet())
            if (columnKeyMap.get(columnName).size() == 1)
                numberOfExclusiveColumnNames++;

        switch (numberOfExclusiveColumnNames) {
            case 0:
                return HiveJoinQueryResolver.EXCLUSIVE_COLUMN_ZERO;
            case 1:
                return HiveJoinQueryResolver.EXCLUSIVE_COLUMN_ONE;
        }

        return HiveJoinQueryResolver.EXCLUSIVE_COLUMN_TWO_OR_MORE;
    }

    @Override
    public HiveTask buildHiveJoinTaskWithOutExtractionTask(HiveJoinParameter hiveJoinParameter,
                                                           HiveCreationTask hiveCreationTask,
                                                           Map<String/*MapKey: Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap) {
        try {
            final String tableNameSplitted[] = hiveJoinParameter.getTableName().split("[_]");
            final String mapKey = tableNameSplitted[tableNameSplitted.length - 1];

            List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(mapKey);
            //noinspection Duplicates
            if (hiveJoinParameterList == null) {
                hiveJoinParameterList = new ArrayList<>();
                hiveJoinParameterList.add(hiveJoinParameter);
                hiveJoinParameterListMap.put(mapKey, hiveJoinParameterList);
            } else {
                hiveJoinParameterList.add(hiveJoinParameter);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - Exception occurs at buildHiveJoinTaskWithOutExtractionTask: %s", currentThreadName, e.getMessage()));
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        if (hiveJoinParameter.getIsCreatable())
            return new HiveTask(hiveCreationTask, null);

        return null;
    }
}