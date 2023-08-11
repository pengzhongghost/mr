package com.redu.mapreduce.util;

import java.math.BigDecimal;

/**
 * @author pengzhong
 * @since 2023/8/11
 */
public class OperatorUtil {


    public static boolean compare(BigDecimal tarNum, BigDecimal cmpNum, String operator) {
        switch (operator) {
            case ">":
                return tarNum.compareTo(cmpNum) > 0;
            case ">=":
                return tarNum.compareTo(cmpNum) >= 0;
            case "<":
                return tarNum.compareTo(cmpNum) < 0;
            case "<=":
                return tarNum.compareTo(cmpNum) <= 0;
        }
        return false;
    }

}
