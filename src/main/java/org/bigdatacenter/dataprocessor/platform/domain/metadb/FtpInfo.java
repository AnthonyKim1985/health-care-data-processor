package org.bigdatacenter.dataprocessor.platform.domain.metadb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by hyuk0 on 2017-06-26.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FtpInfo implements Serializable {
    private Integer dataSetUID;
    private String userID;
    private String ftpURI;
}
