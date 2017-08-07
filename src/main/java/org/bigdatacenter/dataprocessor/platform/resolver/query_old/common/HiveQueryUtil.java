package org.bigdatacenter.dataprocessor.platform.resolver.query_old.common;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-28.
 */
public class HiveQueryUtil {
    public static String getDbAndTableNameForExtractedDataSet(String dbName, String tableName, String hiveQuery) {
        return String.format("%s_extracted.%s_%s", dbName, tableName, DataProcessorUtil.getHashedString(hiveQuery)); // hashed value for hiveQuery
    }

    public static String getIntegratedDbNameForJoinQuery(String dbName, String joinKey) {
        return String.format("%s_join_%s_integrated", dbName, joinKey);
    }

    public static String getIntegratedTableNameForJoinQuery(String tableName, String hiveQuery) {
        return String.format("%s_%s", tableName, DataProcessorUtil.getHashedString(hiveQuery));
    }

    public static String concatDbAndTableName(String dbName, String tableName) {
        return String.format("%s.%s", dbName, tableName);
    }

    public static String concatDbAndTableName(String dbName, String joinKey, String tableName, String hiveQuery) {
        return String.format("%s.%s", getIntegratedDbNameForJoinQuery(dbName, joinKey), getIntegratedTableNameForJoinQuery(tableName, hiveQuery));
    }

    public static String getSelectAllQuery(String dbAndTableName) {
        return String.format("SELECT * FROM %s", dbAndTableName);
    }

    public static String getSelectSomeQuery(String projectionColumns, String dbAndTableName) {
        return String.format("SELECT %s FROM %s", projectionColumns, dbAndTableName);
    }
}
