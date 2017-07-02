package org.bigdatacenter.dataprocessor.platform.domain.hive.common;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-02.
 */
public interface ExtractionParameter<Type> extends Serializable {
    Type getRequestInfo();

    void setRequestInfo(Type requestInfo);

    Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> getParameterMap();

    void setParameterMap(Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap);
}
