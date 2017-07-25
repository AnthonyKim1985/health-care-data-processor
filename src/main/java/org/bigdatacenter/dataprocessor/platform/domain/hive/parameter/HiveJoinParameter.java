package org.bigdatacenter.dataprocessor.platform.domain.hive.parameter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-13.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HiveJoinParameter implements Serializable {
    private String dbName;
    private String tableName;
    private String dbAndHashedTableName;
    private String header;
}
