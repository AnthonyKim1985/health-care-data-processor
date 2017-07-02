package org.bigdatacenter.dataprocessor.platform.domain.hive.version1;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.RequestInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-18.
 */
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@Deprecated
public class ExtractionParameterVersion1 implements ExtractionParameter<RequestInfo> {
    private RequestInfo requestInfo;
    private Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap;

    @Override
    public void setRequestInfo(RequestInfo requestInfo) {
        this.requestInfo = requestInfo;
    }

    @Override
    public RequestInfo getRequestInfo() {
        return requestInfo;
    }

    @Override
    public void setParameterMap(Map<String, Map<String, List<String>>> parameterMap) {
        this.parameterMap = parameterMap;
    }

    @Override
    public Map<String, Map<String, List<String>>> getParameterMap() {
        return parameterMap;
    }
}