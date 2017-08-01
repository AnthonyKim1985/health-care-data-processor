package org.bigdatacenter.dataprocessor.platform.resolver.query.join.builder;

import org.bigdatacenter.dataprocessor.platform.domain.hive.query.HiveJoinParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.query.join.map.key.column.value.ColumnKeyMapValue;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-26.
 */
public interface HiveJoinQueryBuilder {
    List<HiveTask> buildHiveJoinQueryTasks(Integer joinTaskType,
                                           RequestInfo requestInfo,
                                           List<HiveJoinParameter> hiveJoinParameterList,
                                           Map<String/*Column Name*/, List<ColumnKeyMapValue>> yearKeyMapValue);
}
