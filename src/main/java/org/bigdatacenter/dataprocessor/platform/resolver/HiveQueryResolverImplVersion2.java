package org.bigdatacenter.dataprocessor.platform.resolver;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.TaskInfo;
import org.bigdatacenter.dataprocessor.platform.service.metadb.version1.MetadbVersion1Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
@Component
public class HiveQueryResolverImplVersion2 extends HiveQueryResolver {
    @Autowired
    private MetadbVersion1Service metadbService;

    @Override
    public ExtractionParameter buildExtractionParameter(Integer dataSetUID) {
        List<TaskInfo> taskInfoList = new ArrayList<>();
        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> parameterMap = new HashMap<>();

        return null;
    }
}
