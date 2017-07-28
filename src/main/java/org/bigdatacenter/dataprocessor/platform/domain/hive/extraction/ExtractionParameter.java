package org.bigdatacenter.dataprocessor.platform.domain.hive.extraction;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.key.ParameterMapKey;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-18.
 */
@Data
@AllArgsConstructor
public class ExtractionParameter implements Serializable {
    private RequestInfo requestInfo;
    private String indicator;
    private Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> parameterMap;
}