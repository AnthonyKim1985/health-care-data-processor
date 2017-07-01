package org.bigdatacenter.dataprocessor.platform.resolver;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-12.
 */
public interface QueryResolver {
    ExtractionParameter buildExtractionParameter(Integer dataSetUID);

    ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter);
}
