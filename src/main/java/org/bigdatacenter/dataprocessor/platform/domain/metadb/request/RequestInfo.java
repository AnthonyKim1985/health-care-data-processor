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
public class RequestInfo implements Serializable {
    private Integer dataSetUID;
    private Integer dataSetFID;
    private String siteID;
    private Integer menuID;
    private Integer subMenuID;
    private Integer lowID;
    private String datasetName;
    private String datasetSubject;
    private String datasetContents;
    private String userID;
    private Integer groupUID;
    private Integer linkID;
    private String signDate;
    private String processDate;
    private Byte dataState;
    private Byte processState;
    private Byte openState;
    private Byte dataType;
    private Byte whoState;
    private String startDate;
    private String endDate;
    private Integer ranking;
    private String versionInfo;
    private Byte delState;
    private Integer datasetID;
    private String jobStartTime;
    private String jobEndTime;
    private String elapsedTime;
    private Integer joinCondition;
}