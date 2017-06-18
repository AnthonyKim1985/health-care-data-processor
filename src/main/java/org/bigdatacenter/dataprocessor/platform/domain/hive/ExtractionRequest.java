package org.bigdatacenter.dataprocessor.platform.domain.hive;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.RequestInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
@Data
@AllArgsConstructor
public class ExtractionRequest implements Serializable {
    private RequestInfo requestInfo;
    private List<HiveTask> hiveTaskList;
}