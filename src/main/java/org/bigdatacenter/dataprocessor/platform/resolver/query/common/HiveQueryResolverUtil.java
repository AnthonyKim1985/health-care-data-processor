package org.bigdatacenter.dataprocessor.platform.resolver.query.common;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
public final class HiveQueryResolverUtil {
    public static Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> convertTaskInfoListToParameterMap(List<TaskInfo> taskInfoList) {
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = new HashMap<>();

        //
        // TODO: Hive Query 생성을 위한 parameter map 을 생성한다.
        //
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

    public static List<HiveTask> convertParameterMapToHiveTaskList(Integer dataSetUID, Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap) {
        final List<HiveTask> hiveTaskList = new ArrayList<>();

        for (String dbAndTableName : parameterMap.keySet()) {
            StringBuilder hiveQueryBuilder = new StringBuilder();
            hiveQueryBuilder.append(String.format("SELECT * FROM %s WHERE ", dbAndTableName));

            Map<String/*column*/, List<String>/*values*/> conditionMap = parameterMap.get(dbAndTableName);

            List<String> columnNameList = new ArrayList<>();
            columnNameList.addAll(conditionMap.keySet());

            for (int columnIndex = 0; columnIndex < columnNameList.size(); columnIndex++) {
                String columnName = columnNameList.get(columnIndex);
                List<String> values = conditionMap.get(columnName);

                if (values == null || values.size() == 0)
                    return null;

                if (values.size() == 1) {
                    hiveQueryBuilder.append(getEquality(columnName, values.get(0)));
                } else {
                    hiveQueryBuilder.append('(');
                    for (int valueIndex = 0; valueIndex < values.size(); valueIndex++) {
                        hiveQueryBuilder.append(getEquality(columnName, values.get(valueIndex)));

                        if (valueIndex < values.size() - 1)
                            hiveQueryBuilder.append(" OR ");
                    }
                    hiveQueryBuilder.append(')');
                }

                if (columnIndex < columnNameList.size() - 1)
                    hiveQueryBuilder.append(" AND ");
            }

            // /tmp/health_care/{dataSetUID}/{dbAndTableName}/{timeStamp}
            final String hdfsLocation = String.format("/tmp/health_care/%d/%s/%s", dataSetUID,
                    dbAndTableName, String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
            hiveTaskList.add(new HiveTask(hdfsLocation, hiveQueryBuilder.toString()));
        }

        return hiveTaskList;
    }

    private static String getEquality(String columnName, String value) {
        if (DataProcessorUtil.isNumeric(value))
            return String.format("%s = %s", columnName, value);

        return String.format("%s = '%s'", columnName, value);
    }
}
