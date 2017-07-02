package org.bigdatacenter.dataprocessor.platform.domain.hive.version2;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class ExtractionRequestVersion2 implements ExtractionRequest<RequestInfo> {
    private RequestInfo requestInfo;
    private List<HiveTask> hiveTaskList;

    @Override
    public RequestInfo getRequestInfo() {
        return null;
    }

    @Override
    public void setRequestInfo(RequestInfo requestInfo) {

    }

    @Override
    public List<HiveTask> getHiveTaskList() {
        return null;
    }

    @Override
    public void setHiveTaskList(List<HiveTask> hiveTaskList) {

    }
}