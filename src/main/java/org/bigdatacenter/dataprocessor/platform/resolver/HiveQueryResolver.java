package org.bigdatacenter.dataprocessor.platform.resolver;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;

import java.util.List;
import java.util.Map;

/**
 * Created by hyuk0 on 2017-06-12.
 */
public interface HiveQueryResolver {
    Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> buildHiveQueryParameter(Integer dataSetUID);

    List<ExtractionRequest> buildHiveQuery(Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> hiveQueryParameter);
}
