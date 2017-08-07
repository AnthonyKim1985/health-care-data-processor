package org.bigdatacenter.dataprocessor.platform.resolver.query_new.builder.where;

import java.util.List;
import java.util.Map;

public interface WhereClauseBuilder {
    String buildClause(Map<String/*column*/, List<String>/*values*/> parameterMapValue);
}
