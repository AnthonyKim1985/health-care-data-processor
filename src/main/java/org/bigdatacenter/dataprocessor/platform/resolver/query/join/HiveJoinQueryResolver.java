package org.bigdatacenter.dataprocessor.platform.resolver.query.join;

import org.bigdatacenter.dataprocessor.platform.domain.hive.parameter.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-17.
 */
public interface HiveJoinQueryResolver {
    List<HiveTask> buildHiveJoinTasks(Integer joinCondition, Integer dataSetUID);

    HiveTask buildHiveJoinTask(HiveJoinParameter hiveJoinParameter, HiveCreationTask hiveCreationTask);
}
