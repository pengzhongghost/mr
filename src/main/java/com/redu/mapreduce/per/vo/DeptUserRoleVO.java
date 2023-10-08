package com.redu.mapreduce.per.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * @author pengzhong
 * @since 2023/10/8
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeptUserRoleVO implements Serializable {

    private Integer id;
    private Integer deptId;
    private Integer userId;
    private Integer roleId;
    private String type;
    private Byte isDelete;
    private Integer creatorId;
    private Integer modifierId;
    private Date updateTime;
    private Date createTime;

}
