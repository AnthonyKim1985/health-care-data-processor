package org.bigdatacenter.dataprocessor.platform.resolver.query.version1;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.MetaColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.MetaDatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta.MetaTableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.resolver.query.common.HiveQueryResolver;
import org.bigdatacenter.dataprocessor.platform.service.metadb.version1.MetadbVersion1Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
@Qualifier("HiveQueryResolverVersion1Impl")
public class HiveQueryResolverVersion1Impl extends HiveQueryResolver {
    @Autowired
    private MetadbVersion1Service metadbService;

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
        List<RequestFilterInfo> requestFilterInfoList = metadbService.findConditions(dataSetUID);

        if (requestFilterInfoList == null)
            return null;

        for (RequestFilterInfo requestFilterInfo : requestFilterInfoList) {
            //
            // TODO: extract_col_list 에서 dataset_select 의 variableEngName 에 해당하는 extract_col_list 의 모든 컬럼 리스트를 찾는다.
            //
            List<MetaColumnInfo> metaColumnInfoList = metadbService.findColumnInfo(requestFilterInfo.getVariableEngName());

            for (MetaColumnInfo metaColumnInfo : metaColumnInfoList) {
                //
                // TODO: extract_tb_list 에서 etl_idx 에 해당하는 테이블 리스트를 찾는다.
                //
                MetaTableInfo metaTableInfo = metadbService.findTableInfo(metaColumnInfo.getEtl_idx());

                //
                // TODO: extract_db_list 에서 edl_idx 에 해당하는 데이터베이스 리스트를 찾는다.
                //
                MetaDatabaseInfo metaDatabaseInfo = metadbService.findDatabaseInfo(metaTableInfo.getEdl_idx());

                if (!metaDatabaseInfo.getEdl_eng_name().equals(requestInfo.getDatasetName()))
                    continue;

                //
                // TODO: TaskInfo 객체를 생성한다.
                //
                TaskInfo taskInfo = new TaskInfo(metaDatabaseInfo.getEdl_eng_name(),
                        String.format("%s_%s", metaTableInfo.getEtl_eng_name(), requestFilterInfo.getVariableYear()),
                        metaColumnInfo.getEcl_eng_name(),
                        requestFilterInfo.getVariableValue());

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