package org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.meta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-02.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MetaRelationIndicatorWithColumn implements Serializable {
    private Integer eicr_idx;
    private Integer edl_idx;
    private Integer etl_idx;
    private Integer ecl_idx;
    private Integer eil_idx;
}
