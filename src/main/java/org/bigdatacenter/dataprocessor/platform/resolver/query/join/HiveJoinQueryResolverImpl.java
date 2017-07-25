package org.bigdatacenter.dataprocessor.platform.resolver.query.join;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.parameter.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-17.
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HiveJoinQueryResolverImpl implements HiveJoinQueryResolver {
    private static final Logger logger = LoggerFactory.getLogger(HiveJoinQueryResolverImpl.class);
    private final String currentThreadName = Thread.currentThread().getName();
    private final Map<String/*MapKey: Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap;

    public HiveJoinQueryResolverImpl() {
        hiveJoinParameterListMap = new HashMap<>();
    }

    @Override
    public List<HiveTask> buildHiveJoinTasksWithExtractionTasks(ExtractionParameter extractionParameter) {
        List<HiveTask> hiveTaskList = null;

        //
        // TODO: 다음 3개의 CASE 중 수행할 작업을 판별한다.
        //
        Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap = getColumnKeyMap(extractionParameter.getParameterMap());
        switch (getJoinTaskType(columnKeyMap)) {
            case EXCLUSIVE_COLUMN_ZERO: // case 1: 모든 테이블에 있는 컬럼만 필터링 (No Join)
                logger.info(String.format("%s - ", currentThreadName));
                break;
            case EXCLUSIVE_COLUMN_ONE: // case 2: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (1개) 필터링
            case EXCLUSIVE_COLUMN_TWO_OR_MORE: // case 3: 모든 테이블에 있는 컬럼 + 특정 테이블에 있는 컬럼 (2개 이상) 필터링
                hiveTaskList = new ArrayList<>(buildHiveJoinTasksForExclusiveColumn(extractionParameter, columnKeyMap));
                break;
            default:
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

    private List<HiveTask> buildHiveJoinTasksForExclusiveColumn(ExtractionParameter extractionParameter,
                                                                Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap) {
        final Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = extractionParameter.getParameterMap();
        final RequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Integer joinCondition = requestInfo.getJoinCondition();
        final Integer dataSetUID = requestInfo.getDataSetUID();

        final List<String> exclusiveDbAndTableNameList = getExclusiveDbAndTableNameList(columnKeyMap);
        final List<String> joinTargetDbAndTableList = getJoinTargetList(parameterMap, exclusiveDbAndTableNameList);

        List<HiveTask> hiveTaskList = null;

        for (String key : hiveJoinParameterListMap.keySet()) {
            List<HiveJoinParameter> hiveJoinParameterList = hiveJoinParameterListMap.get(key);
            switch (joinCondition) {
                case 0:
                    logger.info(String.format("%s - ", currentThreadName));
                    break;
                case 1: // Take join operation by KEY_SEQ
                    hiveTaskList = new ArrayList<>(getHiveJoinTasks(hiveJoinParameterList, exclusiveDbAndTableNameList, joinTargetDbAndTableList, "key_seq", dataSetUID));
                    break;
                case 2: // Take join operation by PERSON_ID
                    hiveTaskList = new ArrayList<>(getHiveJoinTasks(hiveJoinParameterList, exclusiveDbAndTableNameList, joinTargetDbAndTableList, "person_id", dataSetUID));
                    break;
                default:
                    logger.error(String.format("%s - Invalid join condition: %d", currentThreadName, joinCondition));
                    throw new NullPointerException();
            }
        }

        return hiveTaskList;
    }

    private List<HiveTask> getHiveJoinTasks(List<HiveJoinParameter> hiveJoinParameterList,
                                            List<String> exclusiveDbAndTableNameList,
                                            List<String> joinTargetDbAndTableList,
                                            String joinKey, Integer dataSetUID) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        //
        // TODO: Exclusive Table Pre-join
        //
        List<HiveJoinParameter> exclusiveTableJoinParameterList = new ArrayList<>();
        for (String dbAndTableName : exclusiveDbAndTableNameList) {
            exclusiveTableJoinParameterList.add(getHiveJoinParameterByDbAndTableName(hiveJoinParameterList, dbAndTableName));
        }

        hiveTaskList.add(getExclusiveTableJoinTask(exclusiveTableJoinParameterList, joinKey));

        //
        // TODO: Target Table Join
        //
        List<HiveJoinParameter> targetTableJoinParameterList = new ArrayList<>();
        for (String dbAndTableName : joinTargetDbAndTableList) {
            targetTableJoinParameterList.add(getHiveJoinParameterByDbAndTableName(hiveJoinParameterList, dbAndTableName));
        }

        hiveTaskList.addAll(getTargetTableJoinTasks(targetTableJoinParameterList, joinKey));

        return hiveTaskList;
    }

    private HiveTask getExclusiveTableJoinTask(List<HiveJoinParameter> exclusiveTableJoinParameterList, String joinKey) {

    }

    private List<HiveTask> getTargetTableJoinTasks(List<HiveJoinParameter> targetTableJoinParameterList, String joinKey) {

    }

    private HiveJoinParameter getHiveJoinParameterByDbAndTableName(List<HiveJoinParameter> hiveJoinParameterList, String dbAndTableName) {
        for (HiveJoinParameter hiveJoinParameter : hiveJoinParameterList) {
            String splittedDbAndTableName[] = dbAndTableName.split("[.]");
            if (hiveJoinParameter.getDbName().equals(splittedDbAndTableName[0]))
                if (hiveJoinParameter.getDbName().equals(splittedDbAndTableName[1]))
                    return hiveJoinParameter;
        }
        return null;
    }

    private List<String> getExclusiveDbAndTableNameList(Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap) {
        List<String> exclusiveDbAndTableNameList = new ArrayList<>();
        for (String columnName : columnKeyMap.keySet()) {
            List<String> dbAndTableList = columnKeyMap.get(columnName);
            if (dbAndTableList.size() == 1)
                exclusiveDbAndTableNameList.add(dbAndTableList.get(0));
        }

        return exclusiveDbAndTableNameList;
    }

    private List<String> getJoinTargetList(Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap, List<String> exclusiveDbAndTableNameList) {
        List<String> joinTargetDbAndTableList = new ArrayList<>(parameterMap.keySet());
        joinTargetDbAndTableList.removeAll(exclusiveDbAndTableNameList);

        return joinTargetDbAndTableList;
    }

    @Override
    public HiveTask buildHiveJoinTaskWithOutExtractionTask(HiveJoinParameter hiveJoinParameter, HiveCreationTask hiveCreationTask) {
        final String dbName = hiveJoinParameter.getDbName();
        final String tableName = hiveJoinParameter.getTableName();
        final String hashedDbAndTableName = hiveJoinParameter.getHashedDbAndTableName();
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

//    private List<HiveTask> getHiveJoinTasks(List<HiveJoinParameter> hiveJoinParameterList, String joinKey, Integer dataSetUID) {
//        final List<HiveTask> hiveTaskList = new ArrayList<>();
//
//        for (int i = 0; i < hiveJoinParameterList.size(); i++) {
//            HiveJoinParameter hiveJoinParameter = hiveJoinParameterList.get(i);
//
//            try {
//                final String hiveJoinQuery = getHiveJoinQuery(sortParameterList(hiveJoinParameterList, i), joinKey);
//                final String hashedTableName = String.format("%s_%s", hiveJoinParameter.getTableName(), DataProcessorUtil.getHashedString(hiveJoinQuery));
//                final String integratedHashedDbAndTableName = String.format("%s_join_%s_integrated.%s", hiveJoinParameter.getDbName(), joinKey, hashedTableName);
//
//                logger.info(String.format("%s - HiveJoinQuery: %s", currentThreadName, hiveJoinQuery));
//                logger.info(String.format("%s - HashedTableName: %s", currentThreadName, hashedTableName));
//
//                // /tmp/health_care/{dbAndTableName}/{dataSetUID}/{timeStamp}
//                final String hdfsLocation = String.format("/tmp/health_care/%s.%s/%d/%s", hiveJoinParameter.getDbName(), hiveJoinParameter.getTableName(),
//                        dataSetUID, String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
//                final HiveCreationTask hiveCreationTask = new HiveCreationTask(integratedHashedDbAndTableName, hiveJoinQuery);
//                final HiveExtractionTask hiveExtractionTask = new HiveExtractionTask(hdfsLocation, String.format("SELECT * FROM %s", integratedHashedDbAndTableName), hiveJoinParameter.getHeader());
//
//                hiveTaskList.add(new HiveTask(hiveCreationTask, hiveExtractionTask));
//            } catch (ArrayIndexOutOfBoundsException e) {
//                logger.error(String.format("%s - Exception occurs at getHiveJoinTasks: %s", currentThreadName, e.getMessage()));
//                throw new ArrayIndexOutOfBoundsException(e.getMessage());
//            }
//        }
//
//        return hiveTaskList;
//    }
//
//    private List<HiveJoinParameter> sortParameterList(List<HiveJoinParameter> hiveJoinParameterList, int entryIndex) {
//        final List<HiveJoinParameter> sortedHiveJoinParameterList = new ArrayList<>();
//
//        sortedHiveJoinParameterList.add(hiveJoinParameterList.get(entryIndex));
//
//        for (int i = 0; i < hiveJoinParameterList.size(); i++) {
//            if (i == entryIndex) continue;
//            HiveJoinParameter hiveJoinParameter = hiveJoinParameterList.get(i);
//            sortedHiveJoinParameterList.add(hiveJoinParameter);
//        }
//
//        return sortedHiveJoinParameterList;
//    }
//
//    private String getHiveJoinQuery(List<HiveJoinParameter> hiveJoinParameterList, String joinKey) {
//        final StringBuilder joinQueryBuilder = new StringBuilder();
//
//        try {
//            final String entryAlias = hiveJoinParameterList.get(0).getHashedDbAndTableName().split("[.]")[1].split("[_]")[3];
//            joinQueryBuilder.append(String.format("SELECT DISTINCT %s.* FROM %s %s", entryAlias, hiveJoinParameterList.get(0).getHashedDbAndTableName(), entryAlias));
//
//            for (int i = 0; i < hiveJoinParameterList.size() - 1; i++) {
//                final String currentAlias = hiveJoinParameterList.get(i).getHashedDbAndTableName().split("[.]")[1].split("[_]")[3];
//                final String nextAlias = hiveJoinParameterList.get(i + 1).getHashedDbAndTableName().split("[.]")[1].split("[_]")[3];
//
//                joinQueryBuilder.append(String.format(" INNER JOIN %s %s ON (%s.%s = %s.%s)", hiveJoinParameterList.get(i + 1).getHashedDbAndTableName(), nextAlias, currentAlias, joinKey, nextAlias, joinKey));
//            }
//        } catch (ArrayIndexOutOfBoundsException e) {
//            logger.error(String.format("%s - Exception occurs at getHiveJoinQuery: %s", currentThreadName, e.getMessage()));
//            throw new ArrayIndexOutOfBoundsException(e.getMessage());
//        }
//
//        return joinQueryBuilder.toString();
//    }
}
