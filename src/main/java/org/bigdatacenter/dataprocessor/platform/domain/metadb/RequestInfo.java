package org.bigdatacenter.dataprocessor.platform.domain.metadb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by hyuk0 on 2017-06-12.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RequestInfo implements Serializable {
    private Integer dataSetUID;
    private Integer dataSetFID;
    private String siteID;
    private Integer menuID;
    private String subMenuID;
    private String lowID;
    private String datasetName;
    private String datasetSubject;
    private String datasetContents;
    private Integer userID;
    private Integer groupID;
    private Integer linkID;
    private Date signDate;
    private Date processDate;
    private Integer dataState;
    private Integer processState;
    private Integer openState;
    private Integer dataType;
    private Integer whoState;
    private Date startDate;
    private Date endDate;
    private Integer ranking;
    private String versionInfo;
    private Integer delState;
}
