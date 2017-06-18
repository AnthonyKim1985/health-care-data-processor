package org.bigdatacenter.dataprocessor.platform.resolver;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.*;
import org.bigdatacenter.dataprocessor.platform.service.metadb.MetadbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-08.
 */
@Component
public class HiveQueryResolverImpl implements HiveQueryResolver {
    @Autowired
    private MetadbService metadbService;

    @Override
    public ExtractionParameter buildExtractionParameter(Integer dataSetUID) {
        List<TaskInfo> taskInfoList = new ArrayList<>();
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = new HashMap<>();

        //
        // TODO: dataset_list 에서 dataSetUID 에 해당하는 요청을 찾는다.
        //
        RequestInfo requestInfo = metadbService.findRequest(dataSetUID);

        //
        // TODO: dataset_select DB 에서 dataSetUID 에 해당하는 조건 리스트를 찾는다.
        //
        List<ConditionInfo> conditionInfoList = metadbService.findConditions(dataSetUID);

        for (ConditionInfo conditionInfo : conditionInfoList) {
            //
            // TODO: extract_col_list 에서 dataset_select 의 variableEngName 에 해당하는 extract_col_list 의 모든 컬럼 리스트를 찾는다.
            //
            List<ColumnInfo> columnInfoList = metadbService.findColumnInfo(conditionInfo.getVariableEngName());

            for (ColumnInfo columnInfo : columnInfoList) {
                //
                // TODO: extract_tb_list 에서 etl_idx 에 해당하는 테이블 리스트를 찾는다.
                //
                TableInfo tableInfo = metadbService.findTableInfo(columnInfo.getEtl_idx());

                //
                // TODO: extract_db_list 에서 edl_idx 에 해당하는 데이터베이스 리스트를 찾는다.
                //
                DatabaseInfo databaseInfo = metadbService.findDatabaseInfo(tableInfo.getEdl_idx());

                if (!databaseInfo.getEdl_eng_name().equals(requestInfo.getDatasetName()))
                    continue;

                //
                // TODO: TaskInfo 객체를 생성한다.
                //
                TaskInfo taskInfo = new TaskInfo(databaseInfo.getEdl_eng_name(),
                        String.format("%s_%s", tableInfo.getEtl_eng_name(), conditionInfo.getVariableYear()),
                        columnInfo.getEcl_eng_name(),
                        conditionInfo.getVariableValue());

                taskInfoList.add(taskInfo);
            }
        }

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
        return new ExtractionParameter(requestInfo, parameterMap);
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        List<HiveTask> hiveTaskList = new ArrayList<>();
        RequestInfo requestInfo = extractionParameter.getRequestInfo();
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = extractionParameter.getParameterMap();

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

            // /tmp/health_care/{dataSetUID}/{dbAndTableName}/{timeStamp} -> 이렇게 수정
            final String hdfsLocation = String.format("/tmp/health_care/%d/%s/%s", requestInfo.getDataSetUID(),
                    dbAndTableName, String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
            hiveTaskList.add(new HiveTask(hdfsLocation, hiveQueryBuilder.toString()));
        }

        return new ExtractionRequest(requestInfo, hiveTaskList);
    }

    private String getEquality(String columnName, String value) {
        if (isNumeric(value))
            return String.format("%s = %s", columnName, value);

        return String.format("%s = '%s'", columnName, value);
    }

    private boolean isNumeric(String value) {
        try {
            //noinspection ResultOfMethodCallIgnored
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}