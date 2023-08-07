package com.redu.mapreduce.test;

import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
@Slf4j
public class PerformanceReducer extends Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, EmployeePerformanceVO> {

    private final EmployeePerformanceVO employeePerformance = new EmployeePerformanceVO();

    @Override
    protected void reduce(DimensionVO key, Iterable<EmployeePerformanceVO> values, Reducer<DimensionVO, EmployeePerformanceVO, NullWritable, EmployeePerformanceVO>.Context context) throws IOException, InterruptedException {
        if (0 == key.getUserId()) {
            return;
        }
        for (EmployeePerformanceVO value : values) {
            try {
                employeePerformance.setTeamName(value.getTeamName());
                employeePerformance.setUserId(value.getUserId());
                employeePerformance.setTeamId(value.getTeamId());
                employeePerformance.setBranchName(value.getBranchName());
                employeePerformance.setBranchId(value.getBranchId());
                employeePerformance.setGroupName(value.getGroupName());
                employeePerformance.setGroupId(value.getGroupId());
                employeePerformance.setDeptIdPath(value.getDeptIdPath());
                employeePerformance.setDeptNamePath(value.getDeptNamePath());
                employeePerformance.setEmployeeName(value.getEmployeeName());
                employeePerformance.setStatisticsTime(value.getStatisticsTime());
                employeePerformance.setPlatform(value.getPlatform());
                employeePerformance.setOrderCount(employeePerformance.getOrderCount() + value.getOrderCount());
                employeePerformance.setValidOrderNum(employeePerformance.getValidOrderNum() + value.getValidOrderNum());
                employeePerformance.setGmv(new BigDecimal(null == employeePerformance.getGmv() ? "0" : employeePerformance.getGmv()).add(new BigDecimal("".equals(value.getGmv()) ? "0" : value.getGmv())).toString());
                employeePerformance.setFundOrderCount(employeePerformance.getFundOrderCount() + value.getFundOrderCount());
                employeePerformance.setFundOrderGmv(new BigDecimal(null == employeePerformance.getFundOrderGmv() ? "0" : employeePerformance.getFundOrderGmv()).add(new BigDecimal("".equals(value.getFundOrderGmv()) ? "0" : value.getFundOrderGmv())).toString());
                employeePerformance.setValidServiceIncome(new BigDecimal(null == employeePerformance.getValidServiceIncome() ? "0" : employeePerformance.getValidServiceIncome()).add(new BigDecimal("".equals(value.getValidServiceIncome()) ? "0" : value.getValidServiceIncome())).toString());
                employeePerformance.setRoleType(value.getRoleType());
                employeePerformance.setEmployeeNo(value.getEmployeeNo());
                employeePerformance.setOrderAchievementSum(new BigDecimal(null == employeePerformance.getOrderAchievementSum() ? "0" : employeePerformance.getOrderAchievementSum()).add(new BigDecimal("".equals(value.getOrderAchievementSum()) ? "0" : value.getOrderAchievementSum())).toString());
                employeePerformance.setEstimateServiceIncome(new BigDecimal(null == employeePerformance.getEstimateServiceIncome() ? "0" : employeePerformance.getEstimateServiceIncome()).add(new BigDecimal("".equals(value.getEstimateServiceIncome()) ? "0" : value.getEstimateServiceIncome())).toString());
            } catch (Exception e) {
                log.error("PerformanceReducer reduce", e);
                log.error("PerformanceReducer reduce value:{}", JSONUtil.toJsonStr(value));
                log.error("PerformanceReducer reduce employeePerformance:{}", JSONUtil.toJsonStr(employeePerformance));
            }
        }
        context.write(NullWritable.get(), employeePerformance);
    }

}
