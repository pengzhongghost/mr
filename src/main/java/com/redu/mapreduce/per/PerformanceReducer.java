package com.redu.mapreduce.per;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.TypeReference;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.redu.mapreduce.per.config.PerformanceConfigVO;
import com.redu.mapreduce.per.config.RuleVO;
import com.redu.mapreduce.per.vo.ConfigItemVO;
import com.redu.mapreduce.per.vo.ConfigVO;
import com.redu.mapreduce.util.MapJoinUtil;
import com.redu.mapreduce.util.OperatorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.util.List;
import java.util.Objects;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
@Slf4j
public class PerformanceReducer extends Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, OrcStruct> {

    private final TypeDescription schema =
            TypeDescription.fromString("struct<team_name:string,team_id:int,branch_name:string,branch_id:int,group_name:string,group_id:int,dept_id_path:string,dept_name_path:string,employee_name:string,statistics_time:string,platform:string,order_count:bigint,fund_order_count:bigint,valid_order_num:bigint,gmv:string,fund_order_gmv:string,valid_service_income:string,role_type:int,employee_no:string,order_achievement_sum:string,estimate_service_income:string,user_id:bigint,performance_new:string,ds:string,hired_date:string,is_formal:string>");

    private final OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(schema);

    private Integer partnerPartConfigId;

    private static List<PerformanceConfigVO> partnerPartConfigValues;

    private static final String DS = DateUtil.today();

    @Override
    protected void setup(Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, OrcStruct>.Context context) throws IOException, InterruptedException {
        try {
            //1.获取config表中的相关信息
            URI[] cacheFiles = context.getCacheFiles();
            URI uri01 = cacheFiles[4];
            String dirName01 = uri01.toString().split("/\\*")[0];
            List<ConfigVO> configs = MapJoinUtil.read(dirName01, context.getConfiguration(), ConfigVO.class);
            for (ConfigVO config : configs) {
                //1.招商分成业绩key
                if ("commission_config_partner_order_weight_detail".equals(config.getKey())) {
                    partnerPartConfigId = config.getId();
                }
            }
            //2.获取config_item表中的相关信息
            URI uri02 = cacheFiles[5];
            String dirName02 = uri02.toString().split("/\\*")[0];
            List<ConfigItemVO> configItems = MapJoinUtil.read(dirName02, context.getConfiguration(), ConfigItemVO.class);
            for (ConfigItemVO configItem : configItems) {
                if (0 == configItem.getDeptId() && Objects.equals(partnerPartConfigId, configItem.getConfigId())) {
                    partnerPartConfigValues = JSONUtil.toBean(configItem.getValue(), new TypeReference<List<PerformanceConfigVO>>() {
                    }, false);
                }
            }
        } catch (Exception e) {
            System.out.println("PerformanceReducer setUp: " + e.getMessage());
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
    protected void reduce(DimensionVO key, Iterable<EmployeePerformanceVO> values, Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, OrcStruct>.Context context) throws IOException, InterruptedException {
        if (0 == key.getUserId()) {
            return;
        }
        EmployeePerformanceResultVO performanceResult = new EmployeePerformanceResultVO();
        for (EmployeePerformanceVO value : values) {
            try {
                performanceResult.setTeamName(value.getTeamName());
                performanceResult.setTeamId(value.getTeamId());
                performanceResult.setBranchName(value.getBranchName());
                performanceResult.setBranchId(value.getBranchId());
                performanceResult.setGroupName(value.getGroupName());
                performanceResult.setGroupId(value.getGroupId());
                performanceResult.setDeptIdPath(value.getDeptIdPath());
                performanceResult.setDeptNamePath(value.getDeptNamePath());
                performanceResult.setEmployeeName(value.getEmployeeName());
                performanceResult.setStatisticsTime(value.getStatisticsTime());
                performanceResult.setPlatform(value.getPlatform());
                performanceResult.setRoleType(value.getRoleType());
                performanceResult.setEmployeeNo(value.getEmployeeNo());
                performanceResult.setUserId(value.getUserId());
                performanceResult.setHiredDate(value.getHiredDate());
                performanceResult.setIsFormal(value.getIsFormal());
                performanceResult.setOrderCount(performanceResult.getOrderCount() + value.getOrderCount());
                performanceResult.setFundOrderCount(performanceResult.getFundOrderCount() + value.getFundOrderCount());
                performanceResult.setValidOrderNum(performanceResult.getValidOrderNum() + value.getValidOrderNum());
                performanceResult.setGmv(performanceResult.getGmv().add(new BigDecimal(StrUtil.isEmpty(value.getGmv()) ? "0" : value.getGmv())));
                BigDecimal finalServiceIncome = new BigDecimal(StrUtil.isEmpty(value.getValidServiceIncome()) ? "0" : value.getValidServiceIncome());
                performanceResult.setValidServiceIncome(performanceResult.getValidServiceIncome().add(finalServiceIncome));
                performanceResult.setOrderAchievementSum(performanceResult.getOrderAchievementSum().add(new BigDecimal(StrUtil.isEmpty(value.getOrderAchievementSum()) ? "0" : value.getOrderAchievementSum())));
                performanceResult.setEstimateServiceIncome(performanceResult.getEstimateServiceIncome().add(new BigDecimal(StrUtil.isEmpty(value.getEstimateServiceIncome()) ? "0" : value.getEstimateServiceIncome())));
                performanceResult.setFundOrderGmv(performanceResult.getFundOrderGmv().add(new BigDecimal(StrUtil.isEmpty(value.getFundOrderGmv()) ? "0" : value.getFundOrderGmv())));
                BigDecimal serviceFeeRate = new BigDecimal(StrUtil.isEmpty(value.getServiceFeeRate()) ? "0" : value.getServiceFeeRate()).multiply(new BigDecimal(100));
                BigDecimal commissionWeight = getPartCommissionWeight(serviceFeeRate, value.getPlatform());
                if (null != commissionWeight && 0 != BigDecimal.ZERO.compareTo(finalServiceIncome)) {
                    performanceResult.setPerformanceCommission(performanceResult.getPerformanceCommission().add(finalServiceIncome.multiply(commissionWeight).divide(new BigDecimal("100"), 3, RoundingMode.FLOOR)));
                }
            } catch (Exception e) {
                log.error("PerformanceReducer reduce", e);
                log.error("PerformanceReducer reduce value:{}", JSONUtil.toJsonStr(value));
                log.error("PerformanceReducer reduce employeePerformance:{}", JSONUtil.toJsonStr(value));
            }
        }
        performanceResult.setDs(DS);
        Field[] fields = EmployeePerformanceResultVO.class.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            Object fieldValue = null;
            try {
                fieldValue = field.get(performanceResult);
            } catch (IllegalAccessException e) {
                System.out.println("reducer error: " + e.getMessage());
            }
            if (fieldValue instanceof Integer) {
                orcStruct.setFieldValue(i, new IntWritable((Integer) fieldValue));
            } else if (fieldValue instanceof Long) {
                orcStruct.setFieldValue(i, new LongWritable((Long) fieldValue));
            } else if (fieldValue instanceof String) {
                orcStruct.setFieldValue(i, new Text((String) fieldValue));
            } else if (fieldValue instanceof BigDecimal) {
                BigDecimal value = (BigDecimal) fieldValue;
                orcStruct.setFieldValue(i, new Text(String.valueOf(value)));
            }
        }
        context.write(NullWritable.get(), orcStruct);
    }

}
