package org.bigdatacenter.dataprocessor.platform.resolver.query.version1;

import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version1.ExtractionParameterVersion1;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version1.ExtractionRequestVersion1;
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

import java.sql.Timestamp;
import java.util.ArrayList;
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
    public ExtractionParameterVersion1 buildExtractionParameter(Integer dataSetUID) {
        List<TaskInfo> taskInfoList = new ArrayList<>();

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

        return new ExtractionParameterVersion1(requestInfo, super.convertTaskInfoListToParameterMap(taskInfoList));
    }

    @Override
    @SuppressWarnings({"unchecked", "Duplicates"})
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        List<HiveTask> hiveTaskList = new ArrayList<>();
        RequestInfo requestInfo = (RequestInfo) extractionParameter.getRequestInfo();
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

            // /tmp/health_care/{dataSetUID}/{dbAndTableName}/{timeStamp}
            final String hdfsLocation = String.format("/tmp/health_care/%d/%s/%s", requestInfo.getDataSetUID(),
                    dbAndTableName, String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
            hiveTaskList.add(new HiveTask(hdfsLocation, hiveQueryBuilder.toString()));
        }

        return new ExtractionRequestVersion1(requestInfo, hiveTaskList);
    }
}