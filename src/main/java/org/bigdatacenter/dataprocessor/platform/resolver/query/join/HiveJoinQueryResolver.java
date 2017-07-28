package org.bigdatacenter.dataprocessor.platform.resolver.query.join;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.key.ParameterMapKey;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;

import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-17.
 */
public interface HiveJoinQueryResolver {
    int EXCLUSIVE_COLUMN_ZERO = 0;
    int EXCLUSIVE_COLUMN_ONE = 1;
    int EXCLUSIVE_COLUMN_TWO_OR_MORE = 2;

    List<HiveTask> buildHiveJoinTasksWithExtractionTasks(ExtractionParameter extractionParameter,
                                                         Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap);

    HiveTask buildHiveJoinTaskWithOutExtractionTask(HiveJoinParameter hiveJoinParameter,
                                                    HiveCreationTask hiveCreationTask, Integer year,
                                                    Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap);

    Map<Integer/*Year*/, Map<String/*Column Name*/, List<String/*Table Name*/>>> getColumnKeyMap(Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> parameterMap);

    Integer getJoinTaskType(Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap);
}
