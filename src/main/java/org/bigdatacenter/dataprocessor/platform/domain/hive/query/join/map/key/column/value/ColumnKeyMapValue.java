package org.bigdatacenter.dataprocessor.platform.domain.hive.query.join.map.key.column.value;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-08-01.
 */
@Data
@AllArgsConstructor
public class ColumnKeyMapValue implements Serializable {
    private String dbName;
    private String tableName;
}