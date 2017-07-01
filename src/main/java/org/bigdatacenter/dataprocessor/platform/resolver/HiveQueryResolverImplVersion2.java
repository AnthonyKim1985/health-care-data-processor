package org.bigdatacenter.dataprocessor.platform.resolver;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.service.metadb.MetadbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
@Component
public class HiveQueryResolverImplVersion2 extends HiveQueryResolver {
    @Autowired
    private MetadbService metadbService;

    @Override
    public ExtractionParameter buildExtractionParameter(Integer dataSetUID) {
        return null;
    }
}
