package org.bigdatacenter.dataprocessor.platform.resolver.query_old;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.request.ExtractionRequest;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-02.
 */
public interface HiveQueryResolver {
    ExtractionParameter buildExtractionParameter(Integer dataSetUID);

    ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter);
}
