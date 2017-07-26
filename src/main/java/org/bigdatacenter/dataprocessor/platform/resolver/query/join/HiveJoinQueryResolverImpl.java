package org.bigdatacenter.dataprocessor.platform.resolver.query.join;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.parameter.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.parameter.HiveTaskAndJoinPair;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final String currentThreadName = Thread.currentThread().getName();
    private final Map<String/*MapKey: Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap;

    public HiveJoinQueryResolverImpl() {
        hiveJoinParameterListMap = new HashMap<>();
    }

    @Override
    public List<HiveTask> buildHiveJoinTasksWithExtractionTasks(ExtractionParameter extractionParameter,
                                                                Map<String/*dbAndTableName*/, HiveExtractionTask> hiveExtractionTaskMap) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        //
        // TODO: 다음 3개의 CASE 중 수행할 작업을 판별한다.
        //
        Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap = getColumnKeyMap(extractionParameter.getParameterMap());
        switch (getJoinTaskType(columnKeyMap)) {
            case EXCLUSIVE_COLUMN_ZERO: // case 1: 모든 테이블에 있는 컬럼만 필터링 (No Join)
                logger.info(String.format("%s - Join Task is EXCLUSIVE_COLUMN_ZERO", currentThreadName));
                hiveTaskList.addAll(buildHiveJoinTasksForNonExclusiveColumn(hiveExtractionTaskMap));
                break;
            case EXCLUSIVE_COLUMN_ONE: // case 2: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (1개) 필터링
                logger.info(String.format("%s - Join Task is EXCLUSIVE_COLUMN_ONE", currentThreadName));
                hiveTaskList.addAll(buildHiveJoinTasksForExclusiveColumn(extractionParameter, columnKeyMap, hiveExtractionTaskMap));
                break;
            case EXCLUSIVE_COLUMN_TWO_OR_MORE: // case 3: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (2개 이상) 필터링
                logger.info(String.format("%s - Join Task is EXCLUSIVE_COLUMN_TWO_OR_MORE", currentThreadName));
                hiveTaskList.addAll(buildHiveJoinTasksForExclusiveColumn(extractionParameter, columnKeyMap, hiveExtractionTaskMap));
                break;
            default:
                logger.error(String.format("%s - Invalid join operation option", currentThreadName));
                hiveJoinParameterListMap.clear();
                throw new NullPointerException();
        }

        return hiveTaskList;
    }

    private Map<String/*Column Name*/, List<String/*Table Name*/>> getColumnKeyMap(Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap) {
        Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap = new HashMap<>();

        for (String dbAndTableName : parameterMap.keySet()) {
            for (String columnName : parameterMap.get(dbAndTableName).keySet()) {
                List<String> tableNameList = columnKeyMap.get(columnName);
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

    private Integer getJoinTaskType(Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap) {
        int numberOfExclusiveColumnNames = 0;
        for (String columnName : columnKeyMap.keySet())
            if (columnKeyMap.get(columnName).size() == 1)
                numberOfExclusiveColumnNames++;

        switch (numberOfExclusiveColumnNames) {
            case 0:
                return EXCLUSIVE_COLUMN_ZERO;
            case 1:
                return EXCLUSIVE_COLUMN_ONE;
        }

        return EXCLUSIVE_COLUMN_TWO_OR_MORE;
    }

    private List<HiveTask> buildHiveJoinTasksForNonExclusiveColumn(Map<String/*dbAndTableName*/, HiveExtractionTask> hiveExtractionTaskMap) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();
        for (String dbAndTableName : hiveExtractionTaskMap.keySet())
            hiveTaskList.add(new HiveTask(null, hiveExtractionTaskMap.get(dbAndTableName)));
        hiveJoinParameterListMap.clear();

        return hiveTaskList;
    }

    private List<HiveTask> buildHiveJoinTasksForExclusiveColumn(ExtractionParameter extractionParameter,
                                                                Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap,
                                                                Map<String/*dbAndTableName*/, HiveExtractionTask> hiveExtractionTaskMap) {
        final Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = extractionParameter.getParameterMap();
        final RequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Integer joinCondition = requestInfo.getJoinCondition();
        final Integer dataSetUID = requestInfo.getDataSetUID();

        List<HiveTask> hiveTaskList = null;

        final List<String> exclusiveDbAndTableNameList = getExclusiveDbAndTableNameList(columnKeyMap);
        final List<String> joinTargetDbAndTableList = getJoinTargetList(parameterMap, exclusiveDbAndTableNameList);

        logger.debug(String.format("%s - exclusiveDbAndTableNameList: %s", currentThreadName, exclusiveDbAndTableNameList));
        logger.debug(String.format("%s - joinTargetDbAndTableList: %s", currentThreadName, joinTargetDbAndTableList));

        for (String key : hiveJoinParameterListMap.keySet()) {
            final List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(key);

            logger.debug(String.format("%s - key: %s", currentThreadName, key));
            logger.debug(String.format("%s - hiveJoinParameterList: %s", currentThreadName, hiveJoinParameterList));

            switch (joinCondition) {
                case 0:
                    logger.info(String.format("%s - No Join Operation", currentThreadName));
                    break;
                case 1: // Take join operation by KEY_SEQ
                    hiveTaskList = new ArrayList<>(getHiveJoinTasks(hiveExtractionTaskMap, hiveJoinParameterList, exclusiveDbAndTableNameList, joinTargetDbAndTableList, "key_seq", dataSetUID));
                    break;
                case 2: // Take join operation by PERSON_ID
                    hiveTaskList = new ArrayList<>(getHiveJoinTasks(hiveExtractionTaskMap, hiveJoinParameterList, exclusiveDbAndTableNameList, joinTargetDbAndTableList, "person_id", dataSetUID));
                    break;
                default:
                    logger.error(String.format("%s - Invalid join condition: %d", currentThreadName, joinCondition));
                    throw new NullPointerException();
            }
        }
        hiveJoinParameterListMap.clear();

        return hiveTaskList;
    }

    private List<HiveTask> getHiveJoinTasks(Map<String/*dbAndTableName*/, HiveExtractionTask> hiveExtractionTaskMap,
                                            List<HiveJoinParameter> hiveJoinParameterList,
                                            List<String> exclusiveDbAndTableNameList,
                                            List<String> joinTargetDbAndTableList,
                                            String joinKey, Integer dataSetUID) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        //
        // TODO: Exclusive Table Pre-join
        //

        logger.debug(String.format("%s - exclusiveDbAndTableNameList.size() at getHiveJoinTasks: %d", currentThreadName, exclusiveDbAndTableNameList.size()));
        List<HiveJoinParameter> exclusiveTableJoinParameterList = new ArrayList<>();
        for (String dbAndTableName : exclusiveDbAndTableNameList) {
            logger.debug(String.format("%s - dbAndTableName at getHiveJoinTasks: %s", currentThreadName, dbAndTableName));
            exclusiveTableJoinParameterList.add(getHiveJoinParameterByDbAndTableName(hiveJoinParameterList, dbAndTableName));
            //hiveTaskList.add(new HiveTask(null, hiveExtractionTaskMap.get(dbAndTableName)));
        }

        logger.debug(String.format("%s - exclusiveTableJoinParameterList at getHiveJoinTasks: %s", currentThreadName, exclusiveTableJoinParameterList));

        final String dbName = exclusiveTableJoinParameterList.get(0).getDbName();
        HiveTaskAndJoinPair hiveTaskAndJoinPair = getExclusiveTableJoinTask(exclusiveTableJoinParameterList, dbName, joinKey, dataSetUID);
        hiveTaskList.add(hiveTaskAndJoinPair.getHiveTask());

        //
        // TODO: Target Table Join
        //
//        List<HiveJoinParameter> targetTableJoinParameterList = new ArrayList<>();
//        targetTableJoinParameterList.addAll(exclusiveTableJoinParameterList);
//
//        for (String dbAndTableName : joinTargetDbAndTableList)
//            targetTableJoinParameterList.add(getHiveJoinParameterByDbAndTableName(hiveJoinParameterList, dbAndTableName));

        logger.debug(String.format("%s - hiveJoinParameterList at getHiveJoinTasks: %s", currentThreadName, hiveJoinParameterList));

        hiveTaskList.addAll(getTargetTableJoinTasks(hiveTaskAndJoinPair.getHiveJoinParameter(), hiveJoinParameterList, joinKey, dataSetUID));

        return hiveTaskList;
    }

    private String getExclusiveTableJoinQuery(List<HiveJoinParameter> exclusiveTableJoinParameterList, String joinKey) {
        if (exclusiveTableJoinParameterList == null) {
            logger.error(String.format("%s - exclusiveTableJoinParameterList at getExclusiveTableJoinQuery is null.", currentThreadName));
            throw new NullPointerException();
        } else if (exclusiveTableJoinParameterList.isEmpty()) {
            logger.error(String.format("%s - exclusiveTableJoinParameterList at getExclusiveTableJoinQuery is empty.", currentThreadName));
            throw new NullPointerException();
        }

        final StringBuilder joinQueryBuilder = new StringBuilder();

        if (exclusiveTableJoinParameterList.size() == 1) {
            HiveJoinParameter hiveJoinParameter = exclusiveTableJoinParameterList.get(0);
            joinQueryBuilder.append(String.format("SELECT %s FROM %s", joinKey, hiveJoinParameter.getDbAndHashedTableName()));
        } else {
            try {
                final HiveJoinParameter firstHiveJoinParameter = exclusiveTableJoinParameterList.get(0);
                final String entryTableName[] = firstHiveJoinParameter.getDbAndHashedTableName().split("[.]")[1].split("[_]");
                final String entryAlias = entryTableName[entryTableName.length - 1];
                joinQueryBuilder.append(String.format("SELECT DISTINCT %s.%s FROM %s %s", entryAlias, joinKey, firstHiveJoinParameter.getDbAndHashedTableName(), entryAlias));

                for (int i = 0; i < exclusiveTableJoinParameterList.size() - 1; i++) {
                    final HiveJoinParameter currentJoinParameter = exclusiveTableJoinParameterList.get(i);
                    final HiveJoinParameter nextJoinParameter = exclusiveTableJoinParameterList.get(i + 1);

                    final String currentTableName[] = currentJoinParameter.getDbAndHashedTableName().split("[.]")[1].split("[_]");
                    final String nextTableName[] = nextJoinParameter.getDbAndHashedTableName().split("[.]")[1].split("[_]");

                    final String currentAlias = currentTableName[currentTableName.length - 1];
                    final String nextAlias = nextTableName[nextTableName.length - 1];

                    joinQueryBuilder.append(String.format(" INNER JOIN %s %s ON (%s.%s = %s.%s)", nextJoinParameter.getDbAndHashedTableName(), nextAlias, currentAlias, joinKey, nextAlias, joinKey));
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.error(String.format("%s - Exception occurs at getHiveJoinQuery: %s", currentThreadName, e.getMessage()));
                throw new ArrayIndexOutOfBoundsException(e.getMessage());
            }
        }

        return joinQueryBuilder.toString();
    }

    private HiveTaskAndJoinPair getExclusiveTableJoinTask(List<HiveJoinParameter> exclusiveTableJoinParameterList, String dbName, String joinKey, Integer dataSetUID) {
        if (exclusiveTableJoinParameterList == null) {
            logger.error(String.format("%s - exclusiveTableJoinParameterList at getExclusiveTableJoinTask is null.", currentThreadName));
            throw new NullPointerException();
        } else if (exclusiveTableJoinParameterList.isEmpty()) {
            logger.error(String.format("%s - exclusiveTableJoinParameterList at getExclusiveTableJoinTask is empty.", currentThreadName));
            throw new NullPointerException();
        }

        logger.debug(String.format("%s - exclusiveTableJoinParameterList at getExclusiveTableJoinTask: %s", currentThreadName, exclusiveTableJoinParameterList));

        final String exclusiveJoinedTableName = getExclusiveJoinedTableName(exclusiveTableJoinParameterList, joinKey);
        final String hiveQuery = getExclusiveTableJoinQuery(exclusiveTableJoinParameterList, joinKey);
        final String integratedDbName = String.format("%s_join_%s_integrated", dbName, joinKey);
        final String integratedTableName = String.format("%s_%s", exclusiveJoinedTableName, DataProcessorUtil.getHashedString(hiveQuery));
        final String integratedDbAndHashedTableName = String.format("%s.%s", integratedDbName, integratedTableName);

        HiveTask hiveTask;
        if (exclusiveTableJoinParameterList.size() == 1) {
            final HiveJoinParameter exclusiveTableJoinFirstParameter = exclusiveTableJoinParameterList.get(0);

            final String tableName = exclusiveTableJoinFirstParameter.getTableName();
            final String dbAndTableName = String.format("%s.%s", dbName, tableName);
            final String dbAndHashedTableName = exclusiveTableJoinFirstParameter.getDbAndHashedTableName();
            final String header = exclusiveTableJoinFirstParameter.getHeader();
            final String hdfsLocation = DataProcessorUtil.getHdfsLocation(dbAndTableName, dataSetUID);

            hiveTask = new HiveTask(
                    new HiveCreationTask(integratedDbAndHashedTableName, hiveQuery),
                    new HiveExtractionTask(hdfsLocation, String.format("SELECT * FROM %s", dbAndHashedTableName), header));
        } else {
            hiveTask = new HiveTask(new HiveCreationTask(integratedDbAndHashedTableName, hiveQuery), null);
        }

        HiveJoinParameter hiveJoinParameter = new HiveJoinParameter(integratedDbName, integratedTableName, integratedDbAndHashedTableName, joinKey);

        logger.debug(String.format("%s - hiveTask at getExclusiveTableJoinTask: %s", currentThreadName, hiveTask));
        logger.debug(String.format("%s - hiveJoinParameter at getExclusiveTableJoinTask: %s", currentThreadName, hiveJoinParameter));

        return new HiveTaskAndJoinPair(hiveTask, hiveJoinParameter);
    }

    private String getExclusiveJoinedTableName(List<HiveJoinParameter> exclusiveTableJoinParameterList, String joinKey) {
        StringBuilder tableNameBuilder = new StringBuilder();

        tableNameBuilder.append("exclusive_");
        for (HiveJoinParameter exclusiveTableJoinParameter : exclusiveTableJoinParameterList)
            tableNameBuilder.append(String.format("%s_", exclusiveTableJoinParameter.getTableName()));
        tableNameBuilder.append(String.format("joined_by_%s", joinKey));

        logger.debug(String.format("%s - tableNameBuilder at getExclusiveJoinedTableName: %s", currentThreadName, tableNameBuilder.toString()));

        return tableNameBuilder.toString();
    }

    private List<HiveTask> getTargetTableJoinTasks(HiveJoinParameter sourceJoinParameter,
                                                   List<HiveJoinParameter> targetTableJoinParameterList,
                                                   String joinKey, Integer dataSetUID) {
        List<HiveTask> hiveTaskList = new ArrayList<>();

        for (HiveJoinParameter targetTableJoinParameter : targetTableJoinParameterList) {
            final String dbName = targetTableJoinParameter.getDbName();
            final String tableName = targetTableJoinParameter.getTableName();

            final String hiveQuery = getTargetTableJoinQuery(sourceJoinParameter, targetTableJoinParameter, joinKey);
            final String integratedDbName = String.format("%s_join_%s_integrated", dbName, joinKey);
            final String integratedTableName = String.format("%s_%s", tableName, DataProcessorUtil.getHashedString(hiveQuery));
            final String integratedDbAndHashedTableName = String.format("%s.%s", integratedDbName, integratedTableName);

            final String hdfsLocation = DataProcessorUtil.getHdfsLocation(String.format("%s.%s", dbName, tableName), dataSetUID);
            HiveCreationTask hiveCreationTask = new HiveCreationTask(integratedDbAndHashedTableName, hiveQuery);
            HiveExtractionTask hiveExtractionTask = new HiveExtractionTask(hdfsLocation, String.format("SELECT * FROM %s", integratedDbAndHashedTableName), targetTableJoinParameter.getHeader());

            logger.debug(String.format("%s - hiveCreationTask at getTargetTableJoinTasks: %s", currentThreadName, hiveCreationTask));
            logger.debug(String.format("%s - hiveExtractionTask at getTargetTableJoinTasks: %s", currentThreadName, hiveExtractionTask));

            hiveTaskList.add(new HiveTask(hiveCreationTask, hiveExtractionTask));
        }

        return hiveTaskList;
    }

    private String getTargetTableJoinQuery(HiveJoinParameter sourceJoinParameter,
                                           HiveJoinParameter targetJoinParameter, String joinKey) {
        final StringBuilder joinQueryBuilder = new StringBuilder();

        logger.debug(String.format("%s - sourceJoinParameter.getDbAndHashedTableName() at getTargetTableJoinQuery: %s", currentThreadName, sourceJoinParameter.getDbAndHashedTableName()));
        logger.debug(String.format("%s - targetJoinParameter.getDbAndHashedTableName() at getTargetTableJoinQuery: %s", currentThreadName, targetJoinParameter.getDbAndHashedTableName()));

        final String sourceTableName[] = sourceJoinParameter.getDbAndHashedTableName().split("[.]")[1].split("[_]");
        final String targetTableName[] = targetJoinParameter.getDbAndHashedTableName().split("[.]")[1].split("[_]");

        final String sourceAlias = sourceTableName[sourceTableName.length - 1];
        final String targetAlias = targetTableName[targetTableName.length - 1];

        joinQueryBuilder.append(String.format("SELECT DISTINCT %s.* FROM %s %s INNER JOIN %s %s ON (%s.%s = %s.%s)",
                targetAlias,
                sourceJoinParameter.getDbAndHashedTableName(), sourceAlias,
                targetJoinParameter.getDbAndHashedTableName(), targetAlias,
                sourceAlias, joinKey,
                targetAlias, joinKey));

        logger.debug(String.format("%s - joinQueryBuilder at getTargetTableJoinQuery: %s", currentThreadName, joinQueryBuilder.toString()));

        return joinQueryBuilder.toString();
    }

    private HiveJoinParameter getHiveJoinParameterByDbAndTableName(List<HiveJoinParameter> hiveJoinParameterList, String dbAndTableName) {
        logger.debug(String.format("%s - hiveJoinParameterList at getHiveJoinParameterByDbAndTableName: %s", currentThreadName, hiveJoinParameterList));
        logger.debug(String.format("%s - dbAndTableName at getHiveJoinParameterByDbAndTableName: %s", currentThreadName, dbAndTableName));

        for (HiveJoinParameter hiveJoinParameter : hiveJoinParameterList) {
            String splittedDbAndTableName[] = dbAndTableName.split("[.]");

            if (hiveJoinParameter.getDbName().equals(splittedDbAndTableName[0]))
                if (hiveJoinParameter.getTableName().equals(splittedDbAndTableName[1]))
                    return hiveJoinParameter;
        }
        return null;
    }

    private List<String> getExclusiveDbAndTableNameList(Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap) {
        Map<String, Object> exclusiveDbAndTableNameMap = new HashMap<>();

        for (String columnName : columnKeyMap.keySet()) {
            List<String> dbAndTableList = columnKeyMap.get(columnName);
            if (dbAndTableList.size() == 1)
                exclusiveDbAndTableNameMap.put(dbAndTableList.get(0), null);
        }

        logger.debug(String.format("%s - exclusiveDbAndTableNameMap.size() at getExclusiveDbAndTableNameList: %d", currentThreadName, exclusiveDbAndTableNameMap.size()));

        return new ArrayList<>(exclusiveDbAndTableNameMap.keySet());
    }

    private List<String> getJoinTargetList(Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap, List<String> exclusiveDbAndTableNameList) {
        List<String> joinTargetDbAndTableList = new ArrayList<>(parameterMap.keySet());
        joinTargetDbAndTableList.removeAll(exclusiveDbAndTableNameList);

        logger.debug(String.format("%s - joinTargetDbAndTableList: %s", currentThreadName, joinTargetDbAndTableList));

        return joinTargetDbAndTableList;
    }

    @Override
    public HiveTask buildHiveJoinTaskWithOutExtractionTask(HiveJoinParameter hiveJoinParameter, HiveCreationTask hiveCreationTask) {
        final String dbName = hiveJoinParameter.getDbName();
        final String tableName = hiveJoinParameter.getTableName();
        final String hashedDbAndTableName = hiveJoinParameter.getDbAndHashedTableName();
        final String header = hiveJoinParameter.getHeader();

        try {
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
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - Exception occurs at buildHiveJoinTaskWithOutExtractionTask: %s", currentThreadName, e.getMessage()));
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        return new HiveTask(hiveCreationTask, null);
    }
}