package org.bigdatacenter.dataprocessor.platform.domain.metadb.version1.meta;

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
@Deprecated
public class MetaDatabaseInfo implements Serializable {
    private Integer edl_idx;
    private String edl_org_name;
    private String edl_kor_name;
    private String edl_eng_name;
    private Integer edl_indicator_yn;
}