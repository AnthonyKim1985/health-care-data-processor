package org.bigdatacenter.dataprocessor.platform.domain.metadb.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-12.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskInfo implements Serializable {
    private String databaseName;
    private String tableName;
    private String columnName;

    private String value;
}
