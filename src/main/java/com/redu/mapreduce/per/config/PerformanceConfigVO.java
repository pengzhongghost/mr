package com.redu.mapreduce.per.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author pengzhong
 * @since 2023/8/11
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PerformanceConfigVO {

    private String platform;

    private String type;

    private List<ConfigVO> config;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConfigVO {

        private BigDecimal weight;

        private List<RuleVO> rules;

    }

}
