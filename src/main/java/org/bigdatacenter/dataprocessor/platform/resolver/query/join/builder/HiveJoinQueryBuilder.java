package org.bigdatacenter.dataprocessor.platform.resolver.query.join.builder;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.parameter.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;

import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-26.
 */
public interface HiveJoinQueryBuilder {
    List<HiveTask> buildHiveJoinQueryTasks(ExtractionParameter extractionParameter,
                                           Map<String/*Column Name*/, List<String/*Table Name*/>> columnKeyMap,
                                           Map<String/*MapKey: Year*/, List<HiveJoinParameter>> hiveJoinParameterListMap);
}
