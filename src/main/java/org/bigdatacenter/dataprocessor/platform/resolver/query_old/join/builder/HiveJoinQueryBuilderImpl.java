package org.bigdatacenter.dataprocessor.platform.resolver.query_old.join.builder;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query_old.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query_old.join.map.key.column.value.ColumnKeyMapValue;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.resolver.query_old.common.HiveQueryUtil;
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
 * Created by Anthony Jinhyuk Kim on 2017-07-26.
 */
@Component
public class HiveJoinQueryBuilderImpl implements HiveJoinQueryBuilder {
    private static final Logger logger = LoggerFactory.getLogger(HiveJoinQueryBuilderImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    @Autowired
    private MetadbService metadbService;

    @Override
    public List<HiveTask> buildHiveJoinQueryTasks(Integer joinTaskType,
                                                  RequestInfo requestInfo,
                                                  List<HiveJoinParameter> hiveJoinParameterList,
                                                  Map<String/*Column Name*/, List<ColumnKeyMapValue>> yearKeyMapValue
    ) {
        final Integer joinCondition = requestInfo.getJoinCondition();
        final List<ColumnKeyMapValue> exclusiveColumnKeyMapValueList = getExclusiveDbAndTableNames(yearKeyMapValue);

        logger.debug(String.format("%s - exclusiveDbAndTableNameList: %s", currentThreadName, exclusiveColumnKeyMapValueList));
        logger.debug(String.format("%s - hiveJoinParameterList: %s", currentThreadName, hiveJoinParameterList));

        List<HiveTask> hiveTaskList;

        try {
            switch (joinCondition) {
                case 1: // Take join operation by KEY_SEQ
                    hiveTaskList = new ArrayList<>(getHiveJoinTasks("key_seq", requestInfo, hiveJoinParameterList, exclusiveColumnKeyMapValueList));
                    break;
                case 2: // Take join operation by PERSON_ID
                    hiveTaskList = new ArrayList<>(getHiveJoinTasks("person_id", requestInfo, hiveJoinParameterList, exclusiveColumnKeyMapValueList));
                    break;
                default:
                    throw new NullPointerException(String.format("%s - Invalid join condition: %d", currentThreadName, joinCondition));
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw new NullPointerException(String.format("%s - Exception occurs at buildHiveJoinQueryTasks: %s", currentThreadName, e.getMessage()));
        }

        return hiveTaskList;
    }

    private List<ColumnKeyMapValue> getExclusiveDbAndTableNames(Map<String/*Column Name*/, List<ColumnKeyMapValue>> yearKeyMapValue) {
        Map<ColumnKeyMapValue, Object> exclusiveColumnKeyMapValueMap = new HashMap<>();

        for (String columnName : yearKeyMapValue.keySet()) {
            List<ColumnKeyMapValue> columnKeyMapValueList = yearKeyMapValue.get(columnName);
            if (columnKeyMapValueList.size() == 1)
                exclusiveColumnKeyMapValueMap.put(columnKeyMapValueList.get(0), null);
        }

        logger.debug(String.format("%s - exclusiveDbAndTableNameMap.size() at getExclusiveDbAndTableNames: %d", currentThreadName, exclusiveColumnKeyMapValueMap.size()));

        return new ArrayList<>(exclusiveColumnKeyMapValueMap.keySet());
    }

    private List<HiveTask> getHiveJoinTasks(String joinKey,
                                            RequestInfo requestInfo,
                                            List<HiveJoinParameter> hiveJoinParameterList,
                                            List<ColumnKeyMapValue> exclusiveColumnKeyMapValueList

    ) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        try {
            final Integer dataSetUID = requestInfo.getDataSetUID();
            final String dbName = metadbService.findMetaDatabase(requestInfo.getDatasetID()).getEdl_eng_name();

            logger.debug(String.format("%s - exclusiveDbAndTableNameList.size() at getHiveJoinTasks: %d", currentThreadName, exclusiveColumnKeyMapValueList.size()));
            logger.debug(String.format("%s - hiveJoinParameterList at getHiveJoinTasks: %s", currentThreadName, hiveJoinParameterList));

            //
            // TODO: Exclusive Table Pre-join
            //
            List<HiveJoinParameter> exclusiveTableJoinParameterList = new ArrayList<>();
            for (ColumnKeyMapValue columnKeyMapValue : exclusiveColumnKeyMapValueList)
                exclusiveTableJoinParameterList.add(getHiveJoinParameterByDbAndTableName(hiveJoinParameterList, columnKeyMapValue));

            logger.debug(String.format("%s - exclusiveTableJoinParameterList at getHiveJoinTasks: %s", currentThreadName, exclusiveTableJoinParameterList));

            //
            // TODO: Target Table Join
            //
            HiveJoinParameter sourceJoinParameter = getSourceJoinParameter(hiveTaskList, exclusiveTableJoinParameterList, dbName, joinKey, dataSetUID);
            hiveTaskList.addAll(getTargetTableJoinTasks(sourceJoinParameter, hiveJoinParameterList, joinKey, dataSetUID));
        } catch (Exception e) {
            throw new NullPointerException(e.getMessage());
        }

        return hiveTaskList;
    }

    private HiveJoinParameter getHiveJoinParameterByDbAndTableName(List<HiveJoinParameter> hiveJoinParameterList, ColumnKeyMapValue columnKeyMapValue) {
        logger.debug(String.format("%s - hiveJoinParameterList at getHiveJoinParameterByDbAndTableName: %s", currentThreadName, hiveJoinParameterList));
        logger.debug(String.format("%s - dbAndTableName at getHiveJoinParameterByDbAndTableName: %s", currentThreadName, columnKeyMapValue));

        final String dbName = columnKeyMapValue.getDbName();
        final String tableName = columnKeyMapValue.getTableName();

        for (HiveJoinParameter hiveJoinParameter : hiveJoinParameterList) {
            if (hiveJoinParameter.getDbName().equals(dbName))
                if (hiveJoinParameter.getTableName().equals(tableName))
                    return hiveJoinParameter;
        }

        return null;
    }

    private HiveJoinParameter getSourceJoinParameter(List<HiveTask> hiveTaskList,
                                                     List<HiveJoinParameter> exclusiveTableJoinParameterList,
                                                     String dbName, String joinKey, Integer dataSetUID
    ) {
        if (exclusiveTableJoinParameterList == null) {
            throw new NullPointerException(String.format("%s - exclusiveTableJoinParameterList at getSourceJoinParameter is null.", currentThreadName));
        } else if (exclusiveTableJoinParameterList.isEmpty()) {
            throw new NullPointerException(String.format("%s - exclusiveTableJoinParameterList at getSourceJoinParameter is empty.", currentThreadName));
        }

        logger.debug(String.format("%s - exclusiveTableJoinParameterList at getSourceJoinParameter: %s", currentThreadName, exclusiveTableJoinParameterList));

        final String hiveQuery;
        try {
            hiveQuery = getExclusiveTableJoinQuery(exclusiveTableJoinParameterList, joinKey);
        } catch (NullPointerException e) {
            throw new NullPointerException(e.getMessage());
        }

        final String exclusiveJoinedTableName = getExclusiveJoinedTableName(exclusiveTableJoinParameterList, joinKey);
        final String integratedDbName = HiveQueryUtil.getIntegratedDbNameForJoinQuery(dbName, joinKey);
        final String integratedHashedTableName = HiveQueryUtil.getIntegratedTableNameForJoinQuery(exclusiveJoinedTableName, hiveQuery);
        final String integratedDbAndHashedTableName = HiveQueryUtil.concatDbAndTableName(integratedDbName, integratedHashedTableName);

        final HiveTask hiveTask;
        if (exclusiveTableJoinParameterList.size() == 1) {
            HiveJoinParameter exclusiveTableJoinFirstParameter = exclusiveTableJoinParameterList.get(0);

            final String tableName = exclusiveTableJoinFirstParameter.getTableName();
            final String dbAndTableName = HiveQueryUtil.concatDbAndTableName(dbName, tableName);
            final String dbAndHashedTableName = exclusiveTableJoinFirstParameter.getDbAndHashedTableName();
            final String header = exclusiveTableJoinFirstParameter.getHeader();
            final String hdfsLocation = DataProcessorUtil.getHdfsLocation(dbAndTableName, dataSetUID);

            hiveTask = new HiveTask(
                    new HiveCreationTask(integratedDbAndHashedTableName, hiveQuery),
                    new HiveExtractionTask(hdfsLocation, HiveQueryUtil.getSelectAllQuery(dbAndHashedTableName), header));
        } else {
            hiveTask = new HiveTask(new HiveCreationTask(integratedDbAndHashedTableName, hiveQuery), null);
        }

        hiveTaskList.add(hiveTask);

        HiveJoinParameter hiveJoinParameter = new HiveJoinParameter(
                integratedDbName, integratedHashedTableName, integratedDbAndHashedTableName, joinKey, Boolean.TRUE);

        logger.debug(String.format("%s - hiveTask at getSourceJoinParameter: %s", currentThreadName, hiveTask));
        logger.debug(String.format("%s - hiveJoinParameter at getSourceJoinParameter: %s", currentThreadName, hiveJoinParameter));

        return hiveJoinParameter;
    }

    private String getExclusiveJoinedTableName(List<HiveJoinParameter> exclusiveTableJoinParameterList, String joinKey) {
        final StringBuilder tableNameBuilder = new StringBuilder();

        tableNameBuilder.append("exclusive_");
        for (HiveJoinParameter exclusiveTableJoinParameter : exclusiveTableJoinParameterList)
            tableNameBuilder.append(String.format("%s_", exclusiveTableJoinParameter.getTableName()));
        tableNameBuilder.append(String.format("joined_by_%s", joinKey));

        logger.debug(String.format("%s - tableNameBuilder at getExclusiveJoinedTableName: %s", currentThreadName, tableNameBuilder.toString()));

        return tableNameBuilder.toString();
    }

    private String getExclusiveTableJoinQuery(List<HiveJoinParameter> exclusiveTableJoinParameterList, String joinKey) {
        if (exclusiveTableJoinParameterList == null) {
            throw new NullPointerException(String.format("%s - exclusiveTableJoinParameterList at getExclusiveTableJoinQuery is null.", currentThreadName));
        } else if (exclusiveTableJoinParameterList.isEmpty()) {
            throw new NullPointerException(String.format("%s - exclusiveTableJoinParameterList at getExclusiveTableJoinQuery is empty.", currentThreadName));
        }

        final StringBuilder joinQueryBuilder = new StringBuilder();

        if (exclusiveTableJoinParameterList.size() == 1) {
            HiveJoinParameter hiveJoinParameter = exclusiveTableJoinParameterList.get(0);
            joinQueryBuilder.append(HiveQueryUtil.getSelectSomeQuery(joinKey, hiveJoinParameter.getDbAndHashedTableName()));
        } else {
            final char entryTableAlias = 'A';
            final HiveJoinParameter firstHiveJoinParameter = exclusiveTableJoinParameterList.get(0);

            joinQueryBuilder.append(String.format("SELECT DISTINCT %c.%s FROM %s %c",
                    entryTableAlias, joinKey, firstHiveJoinParameter.getDbAndHashedTableName(), entryTableAlias));

            for (int i = 1; i < exclusiveTableJoinParameterList.size(); i++) {
                final char prevTableAlias = (char) (entryTableAlias + i - 1);
                final char currentTableAlias = (char) (entryTableAlias + i);

                joinQueryBuilder.append(String.format(" INNER JOIN %s %c ON (%c.key_seq = %c.key_seq)",
                        exclusiveTableJoinParameterList.get(i).getDbAndHashedTableName(), currentTableAlias, prevTableAlias, currentTableAlias));
            }
        }

        return joinQueryBuilder.toString();
    }

    private List<HiveTask> getTargetTableJoinTasks(HiveJoinParameter sourceJoinParameter,
                                                   List<HiveJoinParameter> targetTableJoinParameterList,
                                                   String joinKey, Integer dataSetUID
    ) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        for (HiveJoinParameter targetTableJoinParameter : targetTableJoinParameterList) {
            final String dbName = targetTableJoinParameter.getDbName();
            final String tableName = targetTableJoinParameter.getTableName();

            final String hiveQuery = getTargetTableJoinQuery(sourceJoinParameter, targetTableJoinParameter, joinKey);
            final String integratedDbName = HiveQueryUtil.getIntegratedDbNameForJoinQuery(dbName, joinKey);
            final String integratedHashedTableName = HiveQueryUtil.getIntegratedTableNameForJoinQuery(tableName, hiveQuery);
            final String integratedDbAndHashedTableName = HiveQueryUtil.concatDbAndTableName(integratedDbName, integratedHashedTableName);

            final String dbAndTableName = HiveQueryUtil.concatDbAndTableName(dbName, tableName);
            final String hdfsLocation = DataProcessorUtil.getHdfsLocation(dbAndTableName, dataSetUID);
            HiveCreationTask hiveCreationTask = new HiveCreationTask(integratedDbAndHashedTableName, hiveQuery);
            HiveExtractionTask hiveExtractionTask = new HiveExtractionTask(hdfsLocation,
                    HiveQueryUtil.getSelectAllQuery(integratedDbAndHashedTableName), targetTableJoinParameter.getHeader());

            logger.debug(String.format("%s - hiveCreationTask at getTargetTableJoinTasks: %s", currentThreadName, hiveCreationTask));
            logger.debug(String.format("%s - hiveExtractionTask at getTargetTableJoinTasks: %s", currentThreadName, hiveExtractionTask));

            hiveTaskList.add(new HiveTask(hiveCreationTask, hiveExtractionTask));
        }

        return hiveTaskList;
    }

    private String getTargetTableJoinQuery(HiveJoinParameter sourceJoinParameter,
                                           HiveJoinParameter targetJoinParameter, String joinKey) {
        final StringBuilder joinQueryBuilder = new StringBuilder();

        logger.debug(String.format("%s - sourceJoinParameter.getDbAndHashedTableName() at getTargetTableJoinQuery: %s",
                currentThreadName, sourceJoinParameter.getDbAndHashedTableName()));
        logger.debug(String.format("%s - targetJoinParameter.getDbAndHashedTableName() at getTargetTableJoinQuery: %s",
                currentThreadName, targetJoinParameter.getDbAndHashedTableName()));

        joinQueryBuilder.append(String.format("SELECT DISTINCT A.* FROM %s A INNER JOIN %s B ON (A.%s = B.%s)",
                HiveQueryUtil.concatDbAndTableName(targetJoinParameter.getDbName(), targetJoinParameter.getTableName()),
                sourceJoinParameter.getDbAndHashedTableName(), joinKey, joinKey));

        logger.debug(String.format("%s - joinQueryBuilder at getTargetTableJoinQuery: %s", currentThreadName, joinQueryBuilder.toString()));

        return joinQueryBuilder.toString();
    }
}