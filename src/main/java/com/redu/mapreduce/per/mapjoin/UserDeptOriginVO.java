package com.redu.mapreduce.per.mapjoin;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author pengzhong
 * @since 2023/8/25
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserDeptOriginVO {

    private Integer id;
    private Integer fid;
    private String type;
    private Integer reduId;
    private String ext;
    private String fromTable;
    private Date createTime;
    private Date updateTime;
    private Integer creatorId;
    private Integer modifierId;
    private Integer isDelete;
    private Integer status;

}
