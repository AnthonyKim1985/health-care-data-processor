package org.bigdatacenter.dataprocessor.platform.resolver.query.join.builder;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;

import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-26.
 */
public interface HiveJoinQueryBuilder {
    List<HiveTask> buildHiveJoinQueryTasks(ExtractionParameter extractionParameter,
                                           Integer joinTaskType,
                                           Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap,
                                           Map<Integer/*Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap);
}
