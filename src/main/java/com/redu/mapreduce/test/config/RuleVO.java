package com.redu.mapreduce.test.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author pengzhong
 * @since 2023/8/16
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleVO {

    private String operator;

    private BigDecimal value;

}
