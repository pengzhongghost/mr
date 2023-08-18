package com.redu.mapreduce.performance;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.TypeReference;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.redu.mapreduce.performance.config.PerformanceConfigVO;
import com.redu.mapreduce.performance.config.RuleVO;
import com.redu.mapreduce.util.HdfsUtil;
import com.redu.mapreduce.util.OperatorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;


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
public class PerformanceReducer extends Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, EmployeePerformanceVO> {

    private String partnerPartConfigId;

    private static List<PerformanceConfigVO> partnerPartConfigValues;

    private static final String DS = DateUtil.today();

    @Override
    protected void setup(Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, EmployeePerformanceVO>.Context context) throws IOException, InterruptedException {
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
                }
            }
            rows.close();
            // 关流
            IOUtils.closeStream(configItemReader);
        }
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
    protected void reduce(DimensionVO key, Iterable<EmployeePerformanceVO> values, Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, EmployeePerformanceVO>.Context context) throws IOException, InterruptedException {
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
        EmployeePerformanceVO outV = new EmployeePerformanceVO();
        for (EmployeePerformanceVO value : values) {
            try {
                outV.setTeamName(value.getTeamName());
                outV.setTeamId(value.getTeamId());
                outV.setBranchName(value.getBranchName());
                outV.setBranchId(value.getBranchId());
                outV.setGroupName(value.getGroupName());
                outV.setGroupId(value.getGroupId());
                outV.setDeptIdPath(value.getDeptIdPath());
                outV.setDeptNamePath(value.getDeptNamePath());
                outV.setEmployeeName(value.getEmployeeName());
                outV.setStatisticsTime(value.getStatisticsTime());
                outV.setPlatform(value.getPlatform());
                outV.setRoleType(value.getRoleType());
                outV.setEmployeeNo(value.getEmployeeNo());
                outV.setUserId(value.getUserId());


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
            } catch (Exception e) {
                log.error("PerformanceReducer reduce", e);
                log.error("PerformanceReducer reduce value:{}", JSONUtil.toJsonStr(value));
                log.error("PerformanceReducer reduce employeePerformance:{}", JSONUtil.toJsonStr(value));
            }
        }
        outV.setOrderCount(orderCount);
        outV.setFundOrderCount(fundOrderCount);
        outV.setValidOrderNum(validOrderNum);
        outV.setGmv(String.valueOf(gmv));
        outV.setFundOrderGmv(String.valueOf(fundOrderGmv));
        outV.setValidServiceIncome(String.valueOf(validServiceIncome));
        outV.setOrderAchievementSum(String.valueOf(orderAchievementSum));
        outV.setEstimateServiceIncome(String.valueOf(estimateServiceIncome));
        //计算业绩提成
        outV.setPerformanceNew(performanceCommission.toString());
        outV.setDs(DS);
        context.write(NullWritable.get(), outV);
    }

}
