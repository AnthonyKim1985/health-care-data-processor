package org.bigdatacenter.dataprocessor.platform.domain.hive.version1;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.RequestInfo;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@Deprecated
public class ExtractionRequestVersion1 implements ExtractionRequest<RequestInfo> {
    private RequestInfo requestInfo;
    private List<HiveTask> hiveTaskList;

    @Override
    public RequestInfo getRequestInfo() {
        return requestInfo;
    }

    @Override
    public void setRequestInfo(RequestInfo requestInfo) {
        this.requestInfo = requestInfo;
    }

    @Override
    public List<HiveTask> getHiveTaskList() {
        return hiveTaskList;
    }

    @Override
    public void setHiveTaskList(List<HiveTask> hiveTaskList) {
        this.hiveTaskList = hiveTaskList;
    }
}