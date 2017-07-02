package org.bigdatacenter.dataprocessor.platform.domain.hive.common;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-18.
 */
@Data
@AllArgsConstructor
public class HiveTask implements Serializable {
    private String hdfsLocation;
    private String hiveQuery;
}
