package org.bigdatacenter.dataprocessor.platform.resolver.query.common;

import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.common.ExtractionRequest;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-12.
 */
public interface QueryResolver {
    ExtractionParameter buildExtractionParameter(Integer dataSetUID);

    ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter);
}
