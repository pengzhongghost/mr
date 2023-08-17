package com.redu.mapreduce.test.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author pengzhong
 * @since 2023/8/16
 * @desc 基础提成配置类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BaseCommissionConfigVO {

    /**
     * 订单量还是服务费
     */
    private String type;

    /**
     * 提点
     */
    private BigDecimal weight;

    /**
     * 绩效等级
     */
    private String grade;

    private List<RuleVO> rules;

}
