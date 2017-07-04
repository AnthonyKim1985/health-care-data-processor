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
public class MetaFilterInfo implements Serializable {
    private Integer esl_idx;
    private Integer edl_idx;
    private String esl_kor_name;
    private String esl_eng_name;
    private Byte esl_search_type;
    private Byte esl_indicator_yn;
}