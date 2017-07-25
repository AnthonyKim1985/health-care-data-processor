package org.bigdatacenter.dataprocessor.platform.domain.hive.parameter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-25.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HiveTaskAndJoinPair implements Serializable {
    private HiveTask hiveTask;
    private HiveJoinParameter hiveJoinParameter;
}
