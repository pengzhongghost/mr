package com.redu.mapreduce.test;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author pengzhong
 * @since 2023/8/7
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EmployeeVO {

    /**
     * 用户id
     */
    private String userId;

    /**
     * 姓名
     */
    private String name;

    /**
     * 工号
     */
    private String employeeNo;

    /**
     * 入职时间
     */
    private String hiredDate;

    /**
     * 是否正式员工
     */
    private boolean isFormal;

}
