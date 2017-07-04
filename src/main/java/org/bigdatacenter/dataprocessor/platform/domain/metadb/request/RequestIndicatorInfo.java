package org.bigdatacenter.dataprocessor.platform.domain.metadb.request;

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
public class RequestIndicatorInfo implements Serializable {
    private Integer clickIndicatorUID;
    private Integer dataSetUID;
    private Integer indicatorID;
    private String indicatorName;
}