package com.redu.mapreduce.test;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
@Slf4j
public class PerformanceReducer extends Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, OrcStruct> {

    private final TypeDescription schema =
            TypeDescription.fromString("struct<team_name:string,team_id:int,branch_name:string,branch_id:int,group_name:string,group_id:int,dept_id_path:string,dept_name_path:string,employee_name:string,statistics_time:string,platform:string,order_count:bigint,fund_order_count:bigint,valid_order_num:bigint,gmv:string,fund_order_gmv:string,valid_service_income:string,role_type:int,employee_no:string,order_achievement_sum:string,estimate_service_income:string,user_id:bigint>");

    private final OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(schema);

    private final Text text01 = new Text();

    private final Text text02 = new Text();

    private final Text text03 = new Text();

    private final Text text04 = new Text();

    private final Text text05 = new Text();

    private final Text text06 = new Text();

    private final Text text07 = new Text();

    private final Text text08 = new Text();

    private final Text text09 = new Text();

    private final Text text10 = new Text();

    private final Text text11 = new Text();

    private final Text text12 = new Text();

    private final Text text13 = new Text();

    private final Text text14 = new Text();

    private final IntWritable intWritable01 = new IntWritable();

    private final IntWritable intWritable02 = new IntWritable();

    private final IntWritable intWritable03 = new IntWritable();

    private final IntWritable intWritable04 = new IntWritable();

    private final LongWritable loneWritable01 = new LongWritable();

    private final LongWritable loneWritable02 = new LongWritable();

    private final LongWritable loneWritable03 = new LongWritable();

    private final LongWritable loneWritable04 = new LongWritable();

    @Override
    protected void reduce(DimensionVO key, Iterable<EmployeePerformanceVO> values, Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, OrcStruct>.Context context) throws IOException, InterruptedException {
        if (0 == key.getUserId()) {
            return;
        }
        long orderCount = 0;
        long fundOrderCount = 0;
        long validOrderNum = 0;
        BigDecimal gmv = BigDecimal.ZERO;
        BigDecimal fundOrderGmv = BigDecimal.ZERO;
        BigDecimal validServiceIncome = BigDecimal.ZERO;
        BigDecimal orderAchievementSum = BigDecimal.ZERO;
        BigDecimal estimateServiceIncome = BigDecimal.ZERO;
        for (EmployeePerformanceVO value : values) {
            try {
                text01.set(value.getTeamName());
                intWritable01.set(value.getTeamId());
                text02.set(value.getBranchName());
                intWritable02.set(value.getBranchId());
                text03.set(value.getGroupName());
                intWritable03.set(value.getGroupId());
                text04.set(value.getDeptIdPath());
                text05.set(value.getDeptNamePath());
                text06.set(value.getEmployeeName());
                text07.set(value.getStatisticsTime());
                text08.set(value.getPlatform());
                intWritable04.set(value.getRoleType());
                text12.set(value.getEmployeeNo());
                loneWritable04.set(value.getUserId());

                orcStruct.setFieldValue(0, text01);
                orcStruct.setFieldValue(1, intWritable01);
                orcStruct.setFieldValue(2, text02);
                orcStruct.setFieldValue(3, intWritable02);
                orcStruct.setFieldValue(4, text03);
                orcStruct.setFieldValue(5, intWritable03);
                orcStruct.setFieldValue(6, text04);
                orcStruct.setFieldValue(7, text05);
                orcStruct.setFieldValue(8, text06);
                orcStruct.setFieldValue(9, text07);
                orcStruct.setFieldValue(10, text08);
                orderCount += value.getOrderCount();
                fundOrderCount += value.getFundOrderCount();
                validOrderNum += value.getValidOrderNum();
                gmv = gmv.add(new BigDecimal(StrUtil.isEmpty(value.getGmv()) ? "0" : value.getGmv()));
                fundOrderGmv = fundOrderGmv.add(new BigDecimal(StrUtil.isEmpty(value.getFundOrderGmv()) ? "0" : value.getFundOrderGmv()));
                validServiceIncome = validServiceIncome.add(new BigDecimal(StrUtil.isEmpty(value.getValidServiceIncome()) ? "0" : value.getValidServiceIncome()));
                orderAchievementSum = orderAchievementSum.add(new BigDecimal(StrUtil.isEmpty(value.getOrderAchievementSum()) ? "0" : value.getOrderAchievementSum()));
                estimateServiceIncome = estimateServiceIncome.add(new BigDecimal(StrUtil.isEmpty(value.getEstimateServiceIncome()) ? "0" : value.getEstimateServiceIncome()));


                orcStruct.setFieldValue(17, intWritable04);
                orcStruct.setFieldValue(18, text12);
                orcStruct.setFieldValue(21, loneWritable04);

            } catch (Exception e) {
                log.error("PerformanceReducer reduce", e);
                log.error("PerformanceReducer reduce value:{}", JSONUtil.toJsonStr(value));
                log.error("PerformanceReducer reduce employeePerformance:{}", JSONUtil.toJsonStr(value));
            }
        }
        loneWritable01.set(orderCount);
        orcStruct.setFieldValue(11, loneWritable01);
        loneWritable02.set(fundOrderCount);
        orcStruct.setFieldValue(12, loneWritable02);
        loneWritable03.set(validOrderNum);
        orcStruct.setFieldValue(13, loneWritable03);
        text09.set(String.valueOf(gmv));
        orcStruct.setFieldValue(14, text09);
        text10.set(String.valueOf(fundOrderGmv));
        orcStruct.setFieldValue(15, text10);
        text11.set(String.valueOf(validServiceIncome));
        orcStruct.setFieldValue(16, text11);
        text13.set(String.valueOf(orderAchievementSum));
        orcStruct.setFieldValue(19, text13);
        text14.set(String.valueOf(estimateServiceIncome));
        orcStruct.setFieldValue(20, text14);
        context.write(NullWritable.get(), orcStruct);
    }

}
