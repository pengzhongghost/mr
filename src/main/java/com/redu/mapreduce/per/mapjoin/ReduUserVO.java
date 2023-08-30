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
public class ReduUserVO {

    private Integer id;
    private String userCode;
    private String name;
    private String password;
    private String avatar;
    private String gender;
    private String remark;
    private String mobile;
    private String employeeNo;
    private String showName;
    private String flowerName;
    private Integer status;
    private String email;
    private String ext;
    private Date createTime;
    private Date updateTime;
    private Integer creatorId;
    private Integer modifierId;
    private Byte isDelete;
    private String dingUnionId;
    private String dingUserId;
    private String dingMobile;
    private Date hiredDate;

}
