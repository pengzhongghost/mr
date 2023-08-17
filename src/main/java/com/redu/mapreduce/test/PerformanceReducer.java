package com.redu.mapreduce.test;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.TypeReference;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.redu.mapreduce.test.config.BaseCommissionConfigVO;
import com.redu.mapreduce.test.config.PerformanceConfigVO;
import com.redu.mapreduce.test.config.RuleVO;
import com.redu.mapreduce.util.HdfsUtil;
import com.redu.mapreduce.util.OperatorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.util.List;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
@Slf4j
public class PerformanceReducer extends Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, OrcStruct> {

    private final TypeDescription schema =
            TypeDescription.fromString("struct<team_name:string,team_id:int,branch_name:string,branch_id:int,group_name:string,group_id:int,dept_id_path:string,dept_name_path:string,employee_name:string,statistics_time:string,platform:string,order_count:bigint,fund_order_count:bigint,valid_order_num:bigint,gmv:string,fund_order_gmv:string,valid_service_income:string,role_type:int,employee_no:string,order_achievement_sum:string,estimate_service_income:string,user_id:bigint,performance_new:string>");

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

    private final Text text15 = new Text();

    private final IntWritable intWritable01 = new IntWritable();

    private final IntWritable intWritable02 = new IntWritable();

    private final IntWritable intWritable03 = new IntWritable();

    private final IntWritable intWritable04 = new IntWritable();

    private final LongWritable loneWritable01 = new LongWritable();

    private final LongWritable loneWritable02 = new LongWritable();

    private final LongWritable loneWritable03 = new LongWritable();

    private final LongWritable loneWritable04 = new LongWritable();

    private String partnerPartConfigId;

    private String partnerConfigId;

    private String channelConfigId;

    private static List<PerformanceConfigVO> partnerPartConfigValues;

    private static BaseCommissionConfigVO partnerConfigValue;

    private static BaseCommissionConfigVO channelConfigValue;

    @Override
    protected void setup(Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, OrcStruct>.Context context) throws IOException, InterruptedException {
        //1.获取redu_user表中的相关信息
        // 获取缓存的文件，并把文件内容封装到集合
        URI[] cacheFiles = context.getCacheFiles();
        URI uri01 = cacheFiles[3];
        String dirName01 = uri01.toString().split("/\\*")[0];
        List<Path> paths = HdfsUtil.ls(dirName01);
        for (Path path : paths) {
            Reader configReader = OrcFile.createReader(path, OrcFile.readerOptions(context.getConfiguration()));
            // 解析schema
            VectorizedRowBatch configInBatch = configReader.getSchema().createRowBatch();
            // 流解析文件
            //1)user表
            RecordReader rows = configReader.rows();
            while (rows.nextBatch(configInBatch)) {   // 读1个batch
                for (int i = 0; i < configInBatch.size; i++) {
                    // 列式读取
                    String id = String.valueOf(((LongColumnVector) configInBatch.cols[0]).vector[i]);
                    BytesColumnVector keyColumn = (BytesColumnVector) configInBatch.cols[6];
                    String key = new String(keyColumn.vector[i], keyColumn.start[i], keyColumn.length[i]);
                    //1.招商分成业绩key
                    if ("commission_config_partner_order_weight_detail".equals(key)) {
                        partnerPartConfigId = id;
                    }
                    //2.招商业绩key
                    if ("commission_config_partner_detail".equals(key)) {
                        partnerConfigId = id;
                    }
                    //3.渠道业绩key
                    if ("commission_config_channel_detail".equals(key)) {
                        channelConfigId = id;
                    }
                }
            }
            rows.close();
            // 关流
            IOUtils.closeStream(configReader);
        }
        //2.获取redu_user表中的相关信息
        // 获取缓存的文件，并把文件内容封装到集合
        URI uri02 = cacheFiles[4];
        String dirName02 = uri02.toString().split("/\\*")[0];
        List<Path> paths02 = HdfsUtil.ls(dirName02);
        for (Path path : paths02) {
            Reader configItemReader = OrcFile.createReader(path, OrcFile.readerOptions(context.getConfiguration()));
            // 解析schema
            VectorizedRowBatch configItemInBatch = configItemReader.getSchema().createRowBatch();
            // 流解析文件
            //1)user表
            RecordReader rows = configItemReader.rows();
            while (rows.nextBatch(configItemInBatch)) {   // 读1个batch
                for (int i = 0; i < configItemInBatch.size; i++) {
                    // 列式读取
                    String id = String.valueOf(((LongColumnVector) configItemInBatch.cols[6]).vector[i]);
                    String deptId = String.valueOf(((LongColumnVector) configItemInBatch.cols[8]).vector[i]);
                    BytesColumnVector valueColumn = (BytesColumnVector) configItemInBatch.cols[9];
                    String value = new String(valueColumn.vector[i], valueColumn.start[i], valueColumn.length[i]);
                    if ("0".equals(deptId) && partnerPartConfigId.equals(id)) {
                        partnerPartConfigValues = JSONUtil.toBean(value, new TypeReference<List<PerformanceConfigVO>>() {
                        }, false);
                    }
//                    if ("0".equals(deptId) && partnerConfigId.equals(id)) {
//                        List<BaseCommissionConfigVO> partnerConfigValues = JSONUtil.toBean(value, new TypeReference<List<BaseCommissionConfigVO>>() {
//                        }, false);
//                        for (BaseCommissionConfigVO partnerConfigValue : partnerConfigValues) {
//                            if ("estimateServiceIncome".equals(partnerConfigValue.getType())) {
//                                PerformanceReducer.partnerConfigValue = partnerConfigValue;
//                            }
//                        }
//                    }
//                    if ("0".equals(deptId) && channelConfigId.equals(id)) {
//                        List<BaseCommissionConfigVO> channelConfigValues = JSONUtil.toBean(value, new TypeReference<List<BaseCommissionConfigVO>>() {
//                        }, false);
//                        for (BaseCommissionConfigVO channelConfigValue : channelConfigValues) {
//                            if ("orderNum".equals(channelConfigValue.getType())) {
//                                PerformanceReducer.channelConfigValue = channelConfigValue;
//                            }
//                        }
//                    }
                }
            }
            rows.close();
            // 关流
            IOUtils.closeStream(configItemReader);
        }
    }

    /**
     * 获取总提成业绩
     * @param numOrAmount
     * @return
     */
    private BaseCommissionConfigVO.ConfigVO getCommissionWeight(BigDecimal numOrAmount, Integer roleType) {
        BaseCommissionConfigVO commissionConfig;
        if (1 == roleType) {
            commissionConfig = partnerConfigValue;
        } else {
            commissionConfig = channelConfigValue;
        }
        for (BaseCommissionConfigVO.ConfigVO config : commissionConfig.getConfig()) {
            List<RuleVO> rules = config.getRules();
            if (1 == rules.size()) {
                RuleVO rule = rules.get(0);
                if (OperatorUtil.compare(numOrAmount, rule.getValue(), rule.getOperator())) {
                    return config;
                }
            }
            if (2 == rules.size()) {
                RuleVO rule01 = rules.get(0);
                RuleVO rule02 = rules.get(1);
                if (OperatorUtil.compare(numOrAmount, rule01.getValue(), rule01.getOperator())
                        && OperatorUtil.compare(numOrAmount, rule02.getValue(), rule02.getOperator())) {
                    return config;
                }
            }
        }
        return null;
    }

    /**
     * 获取提成加权
     *
     * @param serviceFeeRate
     * @param platform
     * @return
     */
    private BigDecimal getPartCommissionWeight(BigDecimal serviceFeeRate, String platform) {
        switch (platform) {
            case "dy":
                platform = "douyin";
                break;
            case "ks":
                platform = "kuaishou";
                break;
            case "wx":
                platform = "weixin";
                break;
        }
        for (PerformanceConfigVO configValue : partnerPartConfigValues) {
            if (platform.equals(configValue.getPlatform())) {
                for (PerformanceConfigVO.ConfigVO config : configValue.getConfig()) {
                    List<RuleVO> rules = config.getRules();
                    if (CollUtil.isNotEmpty(rules)) {
                        if (1 == rules.size()) {
                            RuleVO rule = rules.get(0);
                            if (OperatorUtil.compare(serviceFeeRate, rule.getValue(), rule.getOperator())) {
                                return config.getWeight();
                            }
                        }
                        if (2 == rules.size()) {
                            RuleVO rule01 = rules.get(0);
                            RuleVO rule02 = rules.get(1);
                            if (OperatorUtil.compare(serviceFeeRate, rule01.getValue(), rule01.getOperator())
                                    && OperatorUtil.compare(serviceFeeRate, rule02.getValue(), rule02.getOperator())) {
                                return config.getWeight();
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

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
        BigDecimal performanceCommission = BigDecimal.ZERO;
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
                BigDecimal finalServiceIncome = new BigDecimal(StrUtil.isEmpty(value.getValidServiceIncome()) ? "0" : value.getValidServiceIncome());
                validServiceIncome = validServiceIncome.add(finalServiceIncome);
                orderAchievementSum = orderAchievementSum.add(new BigDecimal(StrUtil.isEmpty(value.getOrderAchievementSum()) ? "0" : value.getOrderAchievementSum()));
                estimateServiceIncome = estimateServiceIncome.add(new BigDecimal(StrUtil.isEmpty(value.getEstimateServiceIncome()) ? "0" : value.getEstimateServiceIncome()));
                if (!"0.0".equals(value.getServiceFeeRate())) {
                    System.out.println(value);
                }
                BigDecimal serviceFeeRate = new BigDecimal(StrUtil.isEmpty(value.getServiceFeeRate()) ? "0" : value.getServiceFeeRate()).multiply(new BigDecimal(100));
                BigDecimal commissionWeight = getPartCommissionWeight(serviceFeeRate, value.getPlatform());
                if (null != commissionWeight && 0 != BigDecimal.ZERO.compareTo(finalServiceIncome)) {
                    performanceCommission = performanceCommission.add(finalServiceIncome.multiply(commissionWeight).divide(new BigDecimal("100"), 3, RoundingMode.FLOOR));
                }
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
        //计算业绩提成
        text15.set(performanceCommission.toString());
        orcStruct.setFieldValue(22, text15);
        context.write(NullWritable.get(), orcStruct);
    }

}
