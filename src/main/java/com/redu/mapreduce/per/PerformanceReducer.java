package com.redu.mapreduce.per;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.redu.mapreduce.per.vo.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
@Slf4j
public class PerformanceReducer extends Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, OrcStruct> {

    private final TypeDescription schema =
            TypeDescription.fromString(Constant.PER_ORC_STRUCT);

    private final OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(schema);

    private static final String DS = DateUtil.today();

    @Override
    protected void reduce(DimensionVO key, Iterable<EmployeePerformanceVO> values, Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, OrcStruct>.Context context) throws IOException, InterruptedException {
        if (0 == key.getUserId()) {
            return;
        }
        EmployeePerformanceResultVO performanceResult = new EmployeePerformanceResultVO();
        for (EmployeePerformanceVO value : values) {
            try {
                //1.部门信息
                performanceResult.setTeamName(value.getTeamName());
                performanceResult.setTeamId(value.getTeamId());
                performanceResult.setBranchName(value.getBranchName());
                performanceResult.setBranchId(value.getBranchId());
                performanceResult.setGroupName(value.getGroupName());
                performanceResult.setGroupId(value.getGroupId());
                performanceResult.setDeptIdPath(value.getDeptIdPath());
                performanceResult.setDeptNamePath(value.getDeptNamePath());
                //2.钉钉部门信息
                performanceResult.setFirstLevelDeptId(value.getFirstLevelDeptId());
                performanceResult.setSecondLevelDeptId(value.getSecondLevelDeptId());
                performanceResult.setThirdLevelDeptId(value.getThirdLevelDeptId());
                performanceResult.setFourthLevelDeptId(value.getFourthLevelDeptId());
                performanceResult.setFifthLevelDeptId(value.getFifthLevelDeptId());
                performanceResult.setSixthLevelDeptId(value.getSixthLevelDeptId());
                performanceResult.setDingDeptIdPath(value.getDingDeptIdPath());
                performanceResult.setDingDeptNamePath(value.getDingDeptNamePath());
                //3.其他
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
                performanceResult.setValidOrderAchievementSum(performanceResult.getValidOrderAchievementSum().add(new BigDecimal(StrUtil.isEmpty(value.getValidOrderAchievementSum()) ? "0" : value.getValidOrderAchievementSum())));
                performanceResult.setEstimateServiceIncome(performanceResult.getEstimateServiceIncome().add(new BigDecimal(StrUtil.isEmpty(value.getEstimateServiceIncome()) ? "0" : value.getEstimateServiceIncome())));
                performanceResult.setFundOrderGmv(performanceResult.getFundOrderGmv().add(new BigDecimal(StrUtil.isEmpty(value.getFundOrderGmv()) ? "0" : value.getFundOrderGmv())));
                performanceResult.setPerformanceCommission(performanceResult.getPerformanceCommission().add(new BigDecimal(StrUtil.isEmpty(value.getPerformanceCommission()) ? "0" : value.getPerformanceCommission())));
                performanceResult.setExcludeXingTuiValidOrderCount(performanceResult.getExcludeXingTuiValidOrderCount() + value.getExcludeXingTuiValidOrderCount());
                performanceResult.setExcludeXingTuiValidGmv(performanceResult.getExcludeXingTuiValidGmv().add(new BigDecimal(StrUtil.isEmpty(value.getExcludeXingTuiValidGmv()) ? "0" : value.getExcludeXingTuiValidGmv())));
                performanceResult.setExcludeXingTuiValidServiceIncome(performanceResult.getExcludeXingTuiValidServiceIncome().add(new BigDecimal(StrUtil.isEmpty(value.getExcludeXingTuiValidServiceIncome()) ? "0" : value.getExcludeXingTuiValidServiceIncome())));
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
