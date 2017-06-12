package org.bigdatacenter.dataprocessor.platform.persistence.metadb;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.*;

import java.util.List;

/**
 * Created by hyuk0 on 2017-06-08.
 */
@Mapper
public interface MetadbMapper {
    RequestInfo findRequest(@Param("dataSetUID") Integer dataSetUID);

    List<ConditionInfo> findConditions(@Param("dataSetUID") Integer dataSetUID);

    List<ColumnInfo> findColumnInfo(@Param("eclEngName") String eclEngName);

    TableInfo findTableInfo(@Param("etlIdx") Integer etlIdx);

    DatabaseInfo findDatabaseInfo(@Param("edlIdx") Integer edlIdx);
}