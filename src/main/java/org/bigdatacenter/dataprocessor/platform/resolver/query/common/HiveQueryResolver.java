package org.bigdatacenter.dataprocessor.platform.resolver.query.common;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hyuk0 on 2017-07-01.
 */
@Component
public abstract class HiveQueryResolver implements QueryResolver {
    protected Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> convertTaskInfoListToParameterMap(List<TaskInfo> taskInfoList) {
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

    protected String getEquality(String columnName, String value) {
        if (DataProcessorUtil.isNumeric(value))
            return String.format("%s = %s", columnName, value);

        return String.format("%s = '%s'", columnName, value);
    }
}
