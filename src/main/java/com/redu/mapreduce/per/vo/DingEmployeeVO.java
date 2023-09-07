package com.redu.mapreduce.per.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author pengzhong
 * @since 2023/9/7
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DingEmployeeVO {

    private Integer id;

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
    private Date hireDate;

    /**
     * 一级部门
     */
    private String oneGradeDept;

    /**
     * 二级部门
     */
    private String twoGradeDept;

    /**
     * 三级部门
     */
    private String threeGradeDept;

    /**
     * 四级部门
     */
    private String fourGradeDept;

    /**
     * 五级部门
     */
    private String fiveGradeDept;

    /**
     * 六级部门
     */
    private String sixGradeDept;

    /**
     * 职位
     */
    private String job;

    /**
     * 岗位职级
     */
    private String jobGrade;

    /**
     * 是否晋升
     */
    private Boolean isPromotion;

    /**
     * 晋升前职位
     */
    private String beforePromotionJob;

    /**
     * 晋升后职位
     */
    private String afterPromotionJob;

    /**
     * 员工状态
     */
    private String status;

    /**
     * 离职时间
     */
    private Date leftDate;

    /**
     * 手机号
     */
    private String mobile;

    /**
     * 办公地点
     */
    private String base;

    /**
     * 备注
     */
    private String notes;

    /**
     * ds
     */
    private String ds;

}
