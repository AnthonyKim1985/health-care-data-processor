package org.bigdatacenter.dataprocessor.platform.domain.metadb.meta;

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
public class MetaDatabaseInfo implements Serializable {
    private Integer edl_idx;
    private String edl_org_name;
    private String edl_kor_name;
    private String edl_eng_name;
    private Byte edl_indicator_yn;
}
