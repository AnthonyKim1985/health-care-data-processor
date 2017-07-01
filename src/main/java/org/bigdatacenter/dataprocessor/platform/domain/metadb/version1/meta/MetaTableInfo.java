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
public class MetaTableInfo implements Serializable {
    private Integer etl_idx;
    private Integer edl_idx;
    private String etl_kor_name;
    private String etl_eng_name;
}
