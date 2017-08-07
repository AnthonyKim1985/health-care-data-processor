package org.bigdatacenter.dataprocessor.platform.resolver.query_new.builder.where;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//@Component
public class WhereClauseBuilderImpl implements WhereClauseBuilder {

    @Override
    public String buildClause(Map<String/*column*/, List<String>/*values*/> parameterMapValue) {
        final StringBuilder whereClauseBuilder = new StringBuilder("WHERE");
        final List<String> columnNameList = new ArrayList<>(parameterMapValue.keySet());

        for (int i = 0; i < columnNameList.size(); i++) {
            String columnName = columnNameList.get(i);

            if (i < columnNameList.size() - 1)
                whereClauseBuilder.append(" AND ");

        }

        return whereClauseBuilder.toString();
    }
}
