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
public class MetaColumnInfo implements Serializable {
    private Integer ecl_idx;
    private String edl_idx;
    private String etl_idx;
    private String ecl_kor_name;
    private String ecl_eng_name;
    private Integer ecl_search_type;
    private Boolean is_usable;
    private Integer ecl_year;
}
