package com.redu.mapreduce.per.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author pengzhong
 * @since 2023/9/2
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConfigItemVO {

    private Integer id;
    private Byte isDelete;
    private Date createTime;
    private Date updateTime;
    private Integer creatorId;
    private Integer modifierId;
    private Integer configId;
    private Integer userId;
    private Integer deptId;
    private String value;

}
