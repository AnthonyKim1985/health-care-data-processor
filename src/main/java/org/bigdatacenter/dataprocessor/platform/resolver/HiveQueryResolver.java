package org.bigdatacenter.dataprocessor.platform.resolver;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.RequestInfo;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hyuk0 on 2017-07-01.
 */
@Component
public abstract class HiveQueryResolver implements QueryResolver {
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

            // /tmp/health_care/{dataSetUID}/{dbAndTableName}/{timeStamp}
            final String hdfsLocation = String.format("/tmp/health_care/%d/%s/%s", requestInfo.getDataSetUID(),
                    dbAndTableName, String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
            hiveTaskList.add(new HiveTask(hdfsLocation, hiveQueryBuilder.toString()));
        }

        return new ExtractionRequest(requestInfo, hiveTaskList);
    }

    private String getEquality(String columnName, String value) {
        if (DataProcessorUtil.isNumeric(value))
            return String.format("%s = %s", columnName, value);

        return String.format("%s = '%s'", columnName, value);
    }
}
