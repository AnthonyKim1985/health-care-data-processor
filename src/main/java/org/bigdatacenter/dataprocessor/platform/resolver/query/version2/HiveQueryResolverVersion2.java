package org.bigdatacenter.dataprocessor.platform.resolver.query.version2;

import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionParameterVersion2;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionRequestVersion2;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-02.
 */
public interface HiveQueryResolverVersion2 {
    ExtractionParameterVersion2 buildExtractionParameter(Integer dataSetUID);

    ExtractionRequestVersion2 buildExtractionRequest(ExtractionParameterVersion2 extractionParameter);
}
