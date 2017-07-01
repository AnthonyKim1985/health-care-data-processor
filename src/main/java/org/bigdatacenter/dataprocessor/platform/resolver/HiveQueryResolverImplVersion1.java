package org.bigdatacenter.dataprocessor.platform.resolver;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.*;
import org.bigdatacenter.dataprocessor.platform.service.metadb.MetadbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-08.
 */
@Component
@Deprecated
public class HiveQueryResolverImplVersion1 extends HiveQueryResolver {
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

        if (requestInfo == null)
            return null;

        //
        // TODO: dataset_select DB 에서 dataSetUID 에 해당하는 조건 리스트를 찾는다.
        //
        List<ConditionInfo> conditionInfoList = metadbService.findConditions(dataSetUID);

        if (conditionInfoList == null)
            return null;

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
}