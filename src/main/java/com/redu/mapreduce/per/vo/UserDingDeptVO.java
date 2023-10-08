package com.redu.mapreduce.per.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author pengzhong
 * @since 2023/10/8
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserDingDeptVO implements Serializable {

    private Integer userId;

    private String firstLevelDeptId;

    private String secondLevelDeptId;

    private String thirdLevelDeptId;

    private String fourthLevelDeptId;

    private String fifthLevelDeptId;

    private String sixthLevelDeptId;

    private String dingDeptIdPath;

    private String dingDeptNamePath;

}
