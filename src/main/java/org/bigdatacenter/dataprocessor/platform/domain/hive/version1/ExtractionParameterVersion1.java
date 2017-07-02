package org.bigdatacenter.dataprocessor.platform.domain.hive.version1;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.request.RequestInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-18.
 */
@Data
@AllArgsConstructor
@Deprecated
public class ExtractionParameterVersion1 implements Serializable {
    private RequestInfo requestInfo;
    private Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap;
}