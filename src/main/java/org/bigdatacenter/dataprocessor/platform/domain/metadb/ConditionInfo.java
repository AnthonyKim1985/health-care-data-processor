package org.bigdatacenter.dataprocessor.platform.domain.metadb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-08.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConditionInfo implements Serializable {
    private Integer variableUID;
    private Integer dataSetUID;
    private Integer variableYear;
    private Integer indicatorID;
    private String indicatorName;
    private Integer variableID;
    private String variableKorName;
    private String variableEngName;
    private String variableValue;
    private Boolean delState;
}
