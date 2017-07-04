package org.bigdatacenter.dataprocessor.platform.resolver.query;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionRequest;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-02.
 */
public interface HiveQueryResolver {
    ExtractionParameter buildExtractionParameter(Integer dataSetUID);

    ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter);
}
