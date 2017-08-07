package org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.parameter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-28.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParameterMapKey implements Serializable {
    private String dbName;
    private String tableName;
    private Integer year;
}
