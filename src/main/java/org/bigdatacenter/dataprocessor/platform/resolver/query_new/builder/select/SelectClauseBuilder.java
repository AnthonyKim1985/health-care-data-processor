package org.bigdatacenter.dataprocessor.platform.resolver.query_new.builder.select;

public interface SelectClauseBuilder {
    String buildClause(String dbName, String tableName);

    String buildClause(String dbName, String tableName, String projections);
}
