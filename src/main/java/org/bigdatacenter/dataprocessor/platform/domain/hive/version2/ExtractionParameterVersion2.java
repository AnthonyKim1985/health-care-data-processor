package org.bigdatacenter.dataprocessor.platform.domain.hive.version2;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-18.
 */
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class ExtractionParameterVersion2 implements ExtractionParameter<RequestInfo> {
    private RequestInfo requestInfo;
    private Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap;


    @Override
    public void setRequestInfo(RequestInfo requestInfo) {
    }

    @Override
    public RequestInfo getRequestInfo() {
        return null;
    }

    @Override
    public void setParameterMap(Map<String, Map<String, List<String>>> parameterMap) {

    }

    @Override
    public Map<String, Map<String, List<String>>> getParameterMap() {
        return null;
    }
}