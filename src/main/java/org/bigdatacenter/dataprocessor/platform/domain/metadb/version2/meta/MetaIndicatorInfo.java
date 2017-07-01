package org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta;

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
public class MetaIndicatorInfo implements Serializable {
    private Integer eil_idx;
    private Integer edl_idx;
    private Integer etl_idx;
    private String eil_kor_name;
    private String eil_eng_name;
}
