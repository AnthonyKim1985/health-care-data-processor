package org.bigdatacenter.dataprocessor.platform.domain.hive.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-11.
 */
@Data
@AllArgsConstructor
public class HiveTask implements Serializable {
    private HiveCreationTask hiveCreationTask;
    private HiveExtractionTask hiveExtractionTask;
}
