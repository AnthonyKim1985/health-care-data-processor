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
public class MetaTableInfo implements Serializable {
    private Integer etl_idx;
    private Integer edl_idx;
    private String etl_eng_name;
    private String etl_kor_name;
    private Integer tb_year;
}