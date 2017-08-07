package org.bigdatacenter.dataprocessor.platform.resolver.query_new;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter.ParameterMapKey;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.request.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.resolver.query_new.builder.join.JoinClauseBuilder;
import org.bigdatacenter.dataprocessor.platform.resolver.query_new.builder.select.SelectClauseBuilder;
import org.bigdatacenter.dataprocessor.platform.resolver.query_new.builder.where.WhereClauseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-08-04.
 */
//@Component
public class NewHiveQueryResolverImpl implements NewHiveQueryResolver {
    private static final Logger logger = LoggerFactory.getLogger(NewHiveQueryResolverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

//    @Autowired
//    @Qualifier("SelectClauseBuilderImpl")
//    private SelectClauseBuilder selectClauseBuilder;
//
//    @Autowired
//    @Qualifier("WhereClauseBuilderImpl")
//    private WhereClauseBuilder whereClauseBuilder;
//
//    @Autowired
//    @Qualifier("JoinClauseBuilderImpl")
//    private JoinClauseBuilder joinClauseBuilder;

    @Override
    public ExtractionParameter buildExtractionParameter(Integer dataSetUID) {
        return null;
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        final RequestInfo requestInfo = extractionParameter.getRequestInfo();
        final Map<ParameterMapKey, Map<String/*column*/, List<String>/*values*/>> parameterMap = extractionParameter.getParameterMap();

        switch (requestInfo.getJoinCondition()) {
            case JOIN_COND_NONE:
                break;
            case JOIN_COND_KEY_SEQ:
                break;
            case JOIN_COND_PERSON_ID:
                break;
            default:
                throw new NullPointerException();
        }

        return null;
    }
}