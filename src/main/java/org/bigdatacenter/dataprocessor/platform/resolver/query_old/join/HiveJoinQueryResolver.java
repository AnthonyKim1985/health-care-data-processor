package org.bigdatacenter.dataprocessor.platform.resolver.query_old.join;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ParameterMapKey;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query_old.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query_old.join.map.key.column.value.ColumnKeyMapValue;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;

import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-17.
 */
public interface HiveJoinQueryResolver {
    int NON_EXCLUSIVE_COLUMN = 0;
    int ONE_EXCLUSIVE_COLUMN = 1;
    int TWO_OR_MORE_EXCLUSIVE_COLUMNS = 2;

    List<HiveTask> buildHiveJoinTasksWithExtractionTasks(ExtractionParameter extractionParameter,
                                                         Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap,
                                                         Map<Integer/*Year*/, List<HiveTask>> hiveTaskListMapForExtractionTask);

    HiveTask buildHiveJoinTaskWithOutExtractionTask(HiveJoinParameter hiveJoinParameter,
                                                    HiveCreationTask hiveCreationTask, Integer year,
                                                    Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap);

    Map<Integer/*Year*/, Map<String/*Column Name*/, List<ColumnKeyMapValue>>> getYearKeyMap(Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> parameterMap);

    Integer getJoinTaskType(Map<String/*Column Name*/, List<ColumnKeyMapValue>> columnKeyMap);
}
