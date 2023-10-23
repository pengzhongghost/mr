package com.redu.mapreduce.per;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author pengzhong
 * @since 2023/9/2
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmployeePerformanceResultVO {

    private String teamName;

    private int teamId;

    private String branchName;

    private int branchId;

    private String groupName;

    private int groupId;

    private String deptIdPath;

    private String deptNamePath;

    private String employeeName;

    private String statisticsTime;

    private String platform;

    private long orderCount;

    private long fundOrderCount;

    private long validOrderNum;

    private BigDecimal gmv = BigDecimal.ZERO;

    private BigDecimal fundOrderGmv = BigDecimal.ZERO;

    private BigDecimal validServiceIncome = BigDecimal.ZERO;

    private int roleType;

    private String employeeNo;

    private BigDecimal orderAchievementSum = BigDecimal.ZERO;

    private BigDecimal validOrderAchievementSum = BigDecimal.ZERO;

    private BigDecimal estimateServiceIncome = BigDecimal.ZERO;

    private Long userId;

    private BigDecimal performanceCommission = BigDecimal.ZERO;

    private String ds;

    private String hiredDate;

    private String isFormal;

    private String firstLevelDeptId;

    private String secondLevelDeptId;

    private String thirdLevelDeptId;

    private String fourthLevelDeptId;

    private String fifthLevelDeptId;

    private String sixthLevelDeptId;

    private String dingDeptIdPath;

    private String dingDeptNamePath;

    /**
     * 排除星推的订单量
     */
    private long excludeXingTuiValidOrderCount;

    /**
     * 排除星推的GMV
     */
    private BigDecimal excludeXingTuiValidGmv = BigDecimal.ZERO;

    /**
     * 排除星推的有效服务费
     */
    private BigDecimal excludeXingTuiValidServiceIncome = BigDecimal.ZERO;

}
