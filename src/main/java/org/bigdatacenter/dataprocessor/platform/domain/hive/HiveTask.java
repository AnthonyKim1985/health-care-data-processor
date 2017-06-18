package org.bigdatacenter.dataprocessor.platform.domain.hive;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by hyuk0 on 2017-06-18.
 */
@Data
@AllArgsConstructor
public class HiveTask implements Serializable {
    private String hdfsLocation;
    private String hiveQuery;
}
