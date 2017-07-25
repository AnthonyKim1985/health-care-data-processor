package org.bigdatacenter.dataprocessor.platform.domain.hive.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-25.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HiveTaskAndExtractionTaskPair implements Serializable {
    private HiveTask hiveTask;
    private String dbAndTableName;
    private HiveExtractionTask hiveExtractionTask;
}
