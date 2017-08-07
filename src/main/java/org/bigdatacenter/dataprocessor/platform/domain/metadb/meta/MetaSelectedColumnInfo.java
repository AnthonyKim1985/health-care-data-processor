package org.bigdatacenter.dataprocessor.platform.domain.metadb.meta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetaSelectedColumnInfo implements Serializable {
    private Integer selectUID;
    private Integer dataSetUID;
    private Integer edl_idx;
    private Integer ecl_year;
    private String etl_eng_name;
    private String ecl_eng_name;
}
