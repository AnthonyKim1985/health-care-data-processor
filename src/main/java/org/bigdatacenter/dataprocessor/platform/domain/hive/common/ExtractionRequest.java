package org.bigdatacenter.dataprocessor.platform.domain.hive.common;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-02.
 */
public interface ExtractionRequest<Type> extends Serializable {
    Type getRequestInfo();

    void setRequestInfo(Type requestInfo);

    List<HiveTask> getHiveTaskList();

    void setHiveTaskList(List<HiveTask> hiveTaskList);
}
