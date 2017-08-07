package org.bigdatacenter.dataprocessor.platform.resolver.query_new;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.request.ExtractionRequest;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-02.
 */
public interface NewHiveQueryResolver {
    int JOIN_COND_NONE = 0;
    int JOIN_COND_KEY_SEQ = 1;
    int JOIN_COND_PERSON_ID = 2;
    
    ExtractionParameter buildExtractionParameter(Integer dataSetUID);

    ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter);
}
