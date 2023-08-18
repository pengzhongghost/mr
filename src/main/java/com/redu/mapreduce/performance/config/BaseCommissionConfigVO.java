package com.redu.mapreduce.performance.config;

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

    private List<ConfigVO> config;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConfigVO {

        /**
         * 提点
         */
        private BigDecimal commission;

        /**
         * 绩效等级
         */
        private String level;

        private List<RuleVO> rules;

    }

}
