package org.bigdatacenter.dataprocessor.platform.domain.hive;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExtractionRequest implements Serializable {
    private String hdfsLocation;
    private String hiveQuery;
}
