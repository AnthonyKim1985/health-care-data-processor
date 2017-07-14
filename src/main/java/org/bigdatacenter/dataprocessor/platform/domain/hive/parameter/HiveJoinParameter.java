package org.bigdatacenter.dataprocessor.platform.domain.hive.parameter;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-13.
 */
@Data
@AllArgsConstructor
public class HiveJoinParameter implements Serializable {
    private String dbName;
    private String tableName;
    private String hashedDbAndTableName;
    private String header;
}
