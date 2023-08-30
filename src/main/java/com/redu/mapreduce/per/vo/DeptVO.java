package com.redu.mapreduce.per.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author pengzhong
 * @since 2023/8/10
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeptVO {

    private String deptIdPath;

    private String deptNamePath;

    private Integer teamId;

    private String teamName;

    private Integer branchId;

    private String branchName;

    private Integer groupId;

    private String groupName;

}
