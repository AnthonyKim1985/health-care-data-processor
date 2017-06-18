package org.bigdatacenter.dataprocessor.platform.domain.hive;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.RequestInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by hyuk0 on 2017-06-18.
 */
@Data
@AllArgsConstructor
public class ExtractionParameter implements Serializable {
    private RequestInfo requestInfo;
    private Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap;
}