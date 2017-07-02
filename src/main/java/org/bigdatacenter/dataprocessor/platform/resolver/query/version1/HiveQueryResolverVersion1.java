package org.bigdatacenter.dataprocessor.platform.resolver.query.version1;

import org.bigdatacenter.dataprocessor.platform.domain.hive.version1.ExtractionParameterVersion1;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version1.ExtractionRequestVersion1;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-02.
 */
@Deprecated
public interface HiveQueryResolverVersion1 {
    ExtractionParameterVersion1 buildExtractionParameter(Integer dataSetUID);

    ExtractionRequestVersion1 buildExtractionRequest(ExtractionParameterVersion1 extractionParameter);
}
