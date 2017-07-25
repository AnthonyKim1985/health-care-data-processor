package org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-11.
 */
@Data
@AllArgsConstructor
public class HiveCreationTask implements Serializable {
    private String dbAndHashedTableName;
    private String hiveQuery;
}