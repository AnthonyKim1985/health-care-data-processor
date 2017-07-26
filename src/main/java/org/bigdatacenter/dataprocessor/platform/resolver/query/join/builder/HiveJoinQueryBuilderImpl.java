package org.bigdatacenter.dataprocessor.platform.resolver.query.join.builder;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.parameter.HiveJoinParameter;
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
 * Created by Anthony Jinhyuk Kim on 2017-07-26.
 */
@Component
public class HiveJoinQueryBuilderImpl implements HiveJoinQueryBuilder {
    private static final Logger logger = LoggerFactory.getLogger(HiveJoinQueryBuilderImpl.class);
    private final String currentThreadName = Thread.currentThread().getName();

    @Override
    public List<HiveTask> buildHiveJoinQueryTasks(ExtractionParameter extractionParameter,
                                                  Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap,
                                                  Map<String/*MapKey: Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap) {
        final RequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Integer joinCondition = requestInfo.getJoinCondition();
        final Integer dataSetUID = requestInfo.getDataSetUID();

        List<HiveTask> hiveTaskList = null;

        final List<String> exclusiveDbAndTableNameList = getExclusiveDbAndTableNameList(columnKeyMap);
        logger.debug(String.format("%s - exclusiveDbAndTableNameList: %s", currentThreadName, exclusiveDbAndTableNameList));

        for (String key : hiveJoinParameterListMap.keySet()) {
            final List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(key);

            logger.debug(String.format("%s - key: %s", currentThreadName, key));
            logger.debug(String.format("%s - hiveJoinParameterList: %s", currentThreadName, hiveJoinParameterList));
            try {
                switch (joinCondition) {
                    case 0:
                        logger.info(String.format("%s - No Join Operation", currentThreadName));
                        break;
                    case 1: // Take join operation by KEY_SEQ
                        hiveTaskList = new ArrayList<>(getHiveJoinTasks(hiveJoinParameterList, exclusiveDbAndTableNameList, "key_seq", dataSetUID));
                        break;
                    case 2: // Take join operation by PERSON_ID
                        hiveTaskList = new ArrayList<>(getHiveJoinTasks(hiveJoinParameterList, exclusiveDbAndTableNameList, "person_id", dataSetUID));
                        break;
                    default:
                        logger.error(String.format("%s - Invalid join condition: %d", currentThreadName, joinCondition));
                        throw new NullPointerException();
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.error(String.format("%s - Exception occurs at buildHiveJoinQueryTasks: %s", currentThreadName, e.getMessage()));
                throw new ArrayIndexOutOfBoundsException(e.getMessage());
            }
        }

        return hiveTaskList;
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

    private List<HiveTask> getHiveJoinTasks(List<HiveJoinParameter> hiveJoinParameterList,
                                            List<String> exclusiveDbAndTableNameList,
                                            String joinKey, Integer dataSetUID) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        logger.debug(String.format("%s - exclusiveDbAndTableNameList.size() at getHiveJoinTasks: %d", currentThreadName, exclusiveDbAndTableNameList.size()));
        logger.debug(String.format("%s - hiveJoinParameterList at getHiveJoinTasks: %s", currentThreadName, hiveJoinParameterList));

        try {
            //
            // TODO: Exclusive Table Pre-join
            //
            List<HiveJoinParameter> exclusiveTableJoinParameterList = new ArrayList<>();
            for (String dbAndTableName : exclusiveDbAndTableNameList) {
                logger.debug(String.format("%s - dbAndTableName at getHiveJoinTasks: %s", currentThreadName, dbAndTableName));
                exclusiveTableJoinParameterList.add(getHiveJoinParameterByDbAndTableName(hiveJoinParameterList, dbAndTableName));
            }

            logger.debug(String.format("%s - exclusiveTableJoinParameterList at getHiveJoinTasks: %s", currentThreadName, exclusiveTableJoinParameterList));

            //
            // TODO: Target Table Join
            //

            final String dbName = exclusiveTableJoinParameterList.get(0).getDbName();
            HiveJoinParameter sourceJoinParameter = getSourceJoinParameter(hiveTaskList, exclusiveTableJoinParameterList, dbName, joinKey, dataSetUID);
            hiveTaskList.addAll(getTargetTableJoinTasks(sourceJoinParameter, hiveJoinParameterList, joinKey, dataSetUID));
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - Exception occurs at getHiveJoinTasks: %s", currentThreadName, e.getMessage()));
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        return hiveTaskList;
    }

    private HiveJoinParameter getHiveJoinParameterByDbAndTableName(List<HiveJoinParameter> hiveJoinParameterList, String dbAndTableName) {
        logger.debug(String.format("%s - hiveJoinParameterList at getHiveJoinParameterByDbAndTableName: %s", currentThreadName, hiveJoinParameterList));
        logger.debug(String.format("%s - dbAndTableName at getHiveJoinParameterByDbAndTableName: %s", currentThreadName, dbAndTableName));

        try {
            for (HiveJoinParameter hiveJoinParameter : hiveJoinParameterList) {
                String splittedDbAndTableName[] = dbAndTableName.split("[.]");

                if (hiveJoinParameter.getDbName().equals(splittedDbAndTableName[0]))
                    if (hiveJoinParameter.getTableName().equals(splittedDbAndTableName[1]))
                        return hiveJoinParameter;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - Exception occurs at getHiveJoinParameterByDbAndTableName: %s", currentThreadName, e.getMessage()));
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }
        return null;
    }

    private HiveJoinParameter getSourceJoinParameter(List<HiveTask> hiveTaskList, List<HiveJoinParameter> exclusiveTableJoinParameterList, String dbName, String joinKey, Integer dataSetUID) {
        if (exclusiveTableJoinParameterList == null) {
            String errorMessage = String.format("%s - exclusiveTableJoinParameterList at getSourceJoinParameter is null.", currentThreadName);
            logger.error(errorMessage);
            throw new NullPointerException(errorMessage);
        } else if (exclusiveTableJoinParameterList.isEmpty()) {
            String errorMessage = String.format("%s - exclusiveTableJoinParameterList at getSourceJoinParameter is empty.", currentThreadName);
            logger.error(errorMessage);
            throw new NullPointerException(errorMessage);
        }

        logger.debug(String.format("%s - exclusiveTableJoinParameterList at getSourceJoinParameter: %s", currentThreadName, exclusiveTableJoinParameterList));

        final String exclusiveJoinedTableName = getExclusiveJoinedTableName(exclusiveTableJoinParameterList, joinKey);
        final String hiveQuery = getExclusiveTableJoinQuery(exclusiveTableJoinParameterList, joinKey);
        final String integratedDbName = String.format("%s_join_%s_integrated", dbName, joinKey);
        final String integratedTableName = String.format("%s_%s", exclusiveJoinedTableName, DataProcessorUtil.getHashedString(hiveQuery));
        final String integratedDbAndHashedTableName = String.format("%s.%s", integratedDbName, integratedTableName);

        HiveTask hiveTask;
        if (exclusiveTableJoinParameterList.size() == 1) {
            HiveJoinParameter exclusiveTableJoinFirstParameter = exclusiveTableJoinParameterList.get(0);

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

        hiveTaskList.add(hiveTask);

        HiveJoinParameter hiveJoinParameter = new HiveJoinParameter(integratedDbName, integratedTableName, integratedDbAndHashedTableName, joinKey);
        logger.debug(String.format("%s - hiveTask at getSourceJoinParameter: %s", currentThreadName, hiveTask));
        logger.debug(String.format("%s - hiveJoinParameter at getSourceJoinParameter: %s", currentThreadName, hiveJoinParameter));

        return hiveJoinParameter;
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

    private String getExclusiveTableJoinQuery(List<HiveJoinParameter> exclusiveTableJoinParameterList, String joinKey) {
        if (exclusiveTableJoinParameterList == null) {
            String errorMessage = String.format("%s - exclusiveTableJoinParameterList at getExclusiveTableJoinQuery is null.", currentThreadName);
            logger.error(errorMessage);
            throw new NullPointerException(errorMessage);
        } else if (exclusiveTableJoinParameterList.isEmpty()) {
            String errorMessage = String.format("%s - exclusiveTableJoinParameterList at getExclusiveTableJoinQuery is empty.", currentThreadName);
            logger.error(errorMessage);
            throw new NullPointerException(errorMessage);
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

    private List<HiveTask> getTargetTableJoinTasks(HiveJoinParameter sourceJoinParameter,
                                                   List<HiveJoinParameter> targetTableJoinParameterList,
                                                   String joinKey, Integer dataSetUID) {
        List<HiveTask> hiveTaskList = new ArrayList<>();

        try {
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
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - Exception occurs at getTargetTableJoinTasks: %s", currentThreadName, e.getMessage()));
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        return hiveTaskList;
    }

    private String getTargetTableJoinQuery(HiveJoinParameter sourceJoinParameter,
                                           HiveJoinParameter targetJoinParameter, String joinKey) {
        final StringBuilder joinQueryBuilder = new StringBuilder();

        logger.debug(String.format("%s - sourceJoinParameter.getDbAndHashedTableName() at getTargetTableJoinQuery: %s", currentThreadName, sourceJoinParameter.getDbAndHashedTableName()));
        logger.debug(String.format("%s - targetJoinParameter.getDbAndHashedTableName() at getTargetTableJoinQuery: %s", currentThreadName, targetJoinParameter.getDbAndHashedTableName()));

        try {
            final String sourceTableName[] = sourceJoinParameter.getDbAndHashedTableName().split("[.]")[1].split("[_]");
            final String targetTableName[] = targetJoinParameter.getDbAndHashedTableName().split("[.]")[1].split("[_]");

            final String sourceAlias = sourceTableName[sourceTableName.length - 1];
            final String targetAlias = targetTableName[targetTableName.length - 1];

            joinQueryBuilder.append(String.format("SELECT DISTINCT %s.* FROM %s %s INNER JOIN %s %s ON (%s.%s = %s.%s)",
                    targetAlias,
                    targetJoinParameter.getDbAndHashedTableName(), targetAlias,
                    sourceJoinParameter.getDbAndHashedTableName(), sourceAlias,
                    targetAlias, joinKey,
                    sourceAlias, joinKey));

            logger.debug(String.format("%s - joinQueryBuilder at getTargetTableJoinQuery: %s", currentThreadName, joinQueryBuilder.toString()));
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(String.format("%s - Exception occurs at getTargetTableJoinQuery: %s", currentThreadName, e.getMessage()));
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        return joinQueryBuilder.toString();
    }
}