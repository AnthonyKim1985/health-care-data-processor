package org.bigdatacenter.dataprocessor.platform.resolver.query_new.builder;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ExtractionParameter;

public interface ClauseBuilder {
    String buildClause(ExtractionParameter extractionParameter);
}
