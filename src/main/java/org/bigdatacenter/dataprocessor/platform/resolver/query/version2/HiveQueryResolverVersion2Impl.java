package org.bigdatacenter.dataprocessor.platform.resolver.query.version2;

import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionParameterVersion2;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionRequestVersion2;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaColumnInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaDatabaseInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta.MetaTableInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestFilterInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestYearInfo;
import org.bigdatacenter.dataprocessor.platform.resolver.query.common.HiveQueryResolver;
import org.bigdatacenter.dataprocessor.platform.service.metadb.version2.MetadbVersion2Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
@Component
@Qualifier("HiveQueryResolverVersion2Impl")
public class HiveQueryResolverVersion2Impl extends HiveQueryResolver {
    @Autowired
    private MetadbVersion2Service metadbVersion2Service;

    @Override
    public ExtractionParameter buildExtractionParameter(Integer dataSetUID) {
        List<TaskInfo> taskInfoList = new ArrayList<>();
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = new HashMap<>();

        // TODO: find request
        RequestInfo requestInfo = metadbVersion2Service.findRequest(dataSetUID);
        if (requestInfo == null)
            return null;

        // TODO: find request year
        List<RequestYearInfo> requestYearInfoList = metadbVersion2Service.findRequestYears(dataSetUID);
        if (requestYearInfoList == null)
            return null;

        // TODO: find request filters
        List<RequestFilterInfo> requestFilterInfoList = metadbVersion2Service.findRequestFilters(dataSetUID);
        if (requestFilterInfoList == null)
            return null;

        // TODO: find database
        MetaDatabaseInfo metaDatabaseInfo = metadbVersion2Service.findMetaDatabase(requestInfo.getDatasetID());
        if (metaDatabaseInfo == null)
            return null;

        // TODO: make tasks
        for (RequestYearInfo requestYearInfo : requestYearInfoList) {
            for (RequestFilterInfo requestFilterInfo : requestFilterInfoList) {
                // TODO: find column
                List<MetaColumnInfo> metaColumnInfoList = metadbVersion2Service.findMetaColumns(
                        requestInfo.getDatasetID(), requestFilterInfo.getFilterEngName(), Integer.parseInt(requestYearInfo.getYearName()));

                for (MetaColumnInfo metaColumnInfo : metaColumnInfoList) {
                    MetaTableInfo metaTableInfo = metadbVersion2Service.findMetaTable(metaColumnInfo.getEtl_idx());

                    String filterValues = requestFilterInfo.getFilterValues();
                    if (filterValues == null)
                        return null;

                    for (String value : filterValues.split("[,]"))
                        taskInfoList.add(new TaskInfo(metaDatabaseInfo.getEdl_eng_name(), metaTableInfo.getEtl_eng_name(), metaColumnInfo.getEcl_eng_name(), value));
                }
            }
        }

        return new ExtractionParameterVersion2(requestInfo, super.convertTaskInfoListToParameterMap(taskInfoList));
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

        return new ExtractionRequestVersion2(requestInfo, hiveTaskList);
    }
}
