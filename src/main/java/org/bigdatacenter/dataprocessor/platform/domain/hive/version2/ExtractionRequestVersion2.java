package org.bigdatacenter.dataprocessor.platform.domain.hive.version2;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
@Data
@AllArgsConstructor
public class ExtractionRequestVersion2 implements Serializable {
    private RequestInfo requestInfo;
    private List<HiveTask> hiveTaskList;
}