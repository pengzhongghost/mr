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
public class ReduDeptVO {

    private Integer id;
    private Integer parentId;
    private String deptCode;
    private String name;
    private String idPath;
    private String namePath;
    private String ext;
    private Integer status;
    private Integer level;
    private Integer creatorId;
    private Integer modifierId;
    private Date createTime;
    private Date updateTime;
    private Byte isDelete;
    private String dingId;
    private String dingParentId;
    private Byte type;

}
