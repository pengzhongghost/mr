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
public class ConfigVO {

    private Integer id;
    private Byte isDelete;
    private Date createTime;
    private Date updateTime;
    private String creator;
    private String modifier;
    private String key;
    private String name;
    private String defaultValue;
    private Byte status;
    private Byte dataType;
    private String displayValue;
    private String displayType;

}
