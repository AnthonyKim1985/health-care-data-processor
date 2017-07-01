package org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-01.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class YearInfo implements Serializable {
    private Integer clickYearUID;
    private Integer dataSetUID;
    private String yearName;
}
