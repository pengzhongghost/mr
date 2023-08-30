package com.redu.mapreduce.per;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.redu.mapreduce.per.mapjoin.ReduDeptVO;
import com.redu.mapreduce.per.mapjoin.ReduUserVO;
import com.redu.mapreduce.per.mapjoin.UserDeptOriginVO;
import com.redu.mapreduce.per.vo.DeptVO;
import com.redu.mapreduce.per.vo.EmployeeVO;
import com.redu.mapreduce.per.vo.ReduOrderVO;
import com.redu.mapreduce.util.HdfsUtil;
import com.redu.mapreduce.util.MapJoinUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
@Slf4j
public class PerformanceMapper extends Mapper<LongWritable, Text, DimensionVO, EmployeePerformanceVO> {

    private DimensionVO outK = new DimensionVO();

    private EmployeePerformanceVO outV = new EmployeePerformanceVO();

    /**
     * <用户id,用户信息>
     */
    private Map<String, EmployeeVO> userMap = new HashMap<>();

    private final Map<String, String> userDeptOriginMap = new HashMap<>();

    /**
     * <热度小组id,php小组id>用于中转的map
     */
    private final Map<String, String> groupIdMap = new HashMap<>();

    private final Map<String, DeptVO> deptMap = new HashMap<>();

    private String paidMonth;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            //1.获取redu_user表中的相关信息
            URI[] cacheFiles = context.getCacheFiles();
            URI uri01 = cacheFiles[0];
            String dirName01 = uri01.toString().split("/\\*")[0];
            List<ReduUserVO> reduUsers = MapJoinUtil.read(dirName01, context.getConfiguration(), ReduUserVO.class);
            userMap = reduUsers.stream().collect(Collectors.toMap(reduUser -> String.valueOf(reduUser.getId()), reduUser -> {
                EmployeeVO employee = new EmployeeVO();
                employee.setUserId(String.valueOf(reduUser.getId()));
                employee.setEmployeeNo(reduUser.getEmployeeNo());
                employee.setName(reduUser.getName());
                String hiredDateStr = DateUtil.format(reduUser.getHiredDate(), DatePattern.NORM_DATETIME_PATTERN);
                employee.setHiredDate(hiredDateStr);
                boolean isFormal = true;
                if (StrUtil.isNotEmpty(hiredDateStr)) {
                    LocalDateTime formalMonth = DateUtil.parseLocalDateTime(DateUtil.format(DateUtil.parseLocalDateTime(hiredDateStr, DatePattern.NORM_DATETIME_PATTERN), DatePattern.NORM_MONTH_PATTERN), DatePattern.NORM_MONTH_PATTERN).plusMonths(2);
                    LocalDateTime nowMonth = DateUtil.parseLocalDateTime(DateUtil.format(LocalDateTime.now(), DatePattern.NORM_MONTH_PATTERN), DatePattern.NORM_MONTH_PATTERN);
                    isFormal = nowMonth.isAfter(formalMonth);
                }
                employee.setFormal(isFormal);
                return employee;
            }));
            //2.获取user_dept_origin中的信息
            URI uri02 = cacheFiles[1];
            String dirName02 = uri02.toString().split("/\\*")[0];
            List<UserDeptOriginVO> deptOrigins = MapJoinUtil.read(dirName02, context.getConfiguration(), UserDeptOriginVO.class);
            for (UserDeptOriginVO deptOrigin : deptOrigins) {
                String fromTable = deptOrigin.getFromTable();
                Integer fid = deptOrigin.getFid();
                Integer reduId = deptOrigin.getReduId();
                String type = deptOrigin.getType();
                userDeptOriginMap.put(fromTable + "|" + fid, String.valueOf(reduId));
                if ("DEPT".equals(type) && "GROUP".equals(fromTable)) {
                    groupIdMap.put(String.valueOf(reduId), type + "|" + fromTable + "|" + fid);
                }
            }
            //3.获取dept表中的信息
            URI uri03 = cacheFiles[2];
            String dirName03 = uri03.toString().split("/\\*")[0];
            List<ReduDeptVO> reduDepts = MapJoinUtil.read(dirName03, context.getConfiguration(), ReduDeptVO.class);
            for (ReduDeptVO dept : reduDepts) {
                String key = groupIdMap.get(String.valueOf(dept.getId()));
                if (StrUtil.isNotEmpty(key)) {
                    String idPath = dept.getIdPath();
                    String[] idSplit = idPath.split("/");
                    String namePath = dept.getNamePath();
                    String[] nameSplit = namePath.split("/");
                    DeptVO deptResult = new DeptVO();
                    if (idSplit.length > 2) {
                        deptResult.setTeamId(Integer.parseInt(idSplit[2]));
                        deptResult.setTeamName(nameSplit[2]);
                        if (idSplit.length > 3) {
                            deptResult.setBranchId(Integer.parseInt(idSplit[3]));
                            deptResult.setBranchName(nameSplit[3]);
                            if (idSplit.length > 4) {
                                deptResult.setGroupId(Integer.parseInt(idSplit[4]));
                                deptResult.setGroupName(nameSplit[4]);
                            }
                        }
                    }
                    deptResult.setDeptIdPath(idPath);
                    deptResult.setDeptNamePath(namePath);
                    deptMap.put(key, deptResult);
                }
            }
            //4.付款时间
            paidMonth = context.getConfiguration().get("paid_month");
        } catch (Exception e) {
            System.out.println("PerformanceMapper setUp: " + e.getMessage());
        }
    }


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DimensionVO, EmployeePerformanceVO>.Context context) throws IOException, InterruptedException {
        outV = new EmployeePerformanceVO();
        String line = value.toString();
        try {
            String[] split = line.split("\u0001");
            ReduOrderVO reduOrder = ReduOrderVO.class.newInstance();
            Field[] fields = ReduOrderVO.class.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.set(reduOrder, split[i]);
            }
            Byte platformType = reduOrder.getPlatformType();
            List<String> appletList = Arrays.asList(reduOrder.getAppletPeg().split(","));
            //1.平台
            if (null == platformType) {
                return;
            }
            if ((1 == platformType && appletList.contains("1"))
                    || 2 == platformType && appletList.contains("0")
                    || 4 == platformType) {
                switch (platformType) {
                    case 1:
                        outK.setPlatform("dy");
                        break;
                    case 2:
                        outK.setPlatform("ks");
                        break;
                    case 4:
                        outK.setPlatform("wx");
                        break;
                    default:
                        outK.setPlatform("-");
                }
                outV.setPlatform(outK.getPlatform());
                OrderExtVO orderExt = JSONUtil.toBean(reduOrder.getExt(), OrderExtVO.class);
                String partnerId = userDeptOriginMap.get("PARTNER|" + orderExt.getHiPartnerid());
                if (StrUtil.isEmpty(partnerId) || "0".equals(partnerId)) {
                    return;
                }
                String statisticsTime = DateUtil.format(reduOrder.getPaidTime(), DatePattern.NORM_MONTH_FORMATTER);
                if (!paidMonth.equals(statisticsTime)) {
                    return;
                }
                Double estimateSettlementAmount = reduOrder.getEstimateSettlementAmount();
                //2.订单数量和gmv
                if (4 == reduOrder.getOrderStatus()) {
                    outV.setFundOrderCount(1);
                    outV.setFundOrderGmv(String.valueOf(estimateSettlementAmount));
                } else {
                    outV.setValidOrderNum(1);
                    outV.setValidServiceIncome(orderExt.getPartnerFinalServiceIncome());
                    outV.setOrderAchievementSum(String.valueOf(reduOrder.getAchievementsOrderMultiple()));
                }
                outV.setGmv(String.valueOf(estimateSettlementAmount));
                outV.setEstimateServiceIncome(String.valueOf(reduOrder.getEstimateServiceIncome()));
                outV.setServiceFeeRate(String.valueOf(reduOrder.getServiceRate()));
                //3.招商
                if (NumberUtil.isNumber(partnerId)) {
                    outV.setUserId(Long.parseLong(partnerId));
                    outK.setUserId(Long.parseLong(partnerId));
                } else {
                    outK.setUserId(0);
                }
                outV.setRoleType(1);
                outV.setOrderCount(1);
                //工号
                EmployeeVO employee = userMap.get(partnerId);
                if (null != employee) {
                    outV.setEmployeeNo(employee.getEmployeeNo());
                    outV.setEmployeeName(employee.getName());
                    outK.setEmployeeNo(employee.getEmployeeNo());
                    outV.setHiredDate(employee.getHiredDate());
                    outV.setIsFormal(String.valueOf(employee.isFormal()));
                } else {
                    outK.setEmployeeNo("0");
                }
                //4.部门信息
                DeptVO dept = deptMap.get("DEPT|GROUP|" + orderExt.getPartnerGroupId());
                if (null != dept) {
                    outV.setDeptIdPath(dept.getDeptIdPath());
                    outV.setTeamId(dept.getTeamId());
                    outV.setBranchId(dept.getBranchId());
                    outV.setGroupId(dept.getGroupId());
                    outV.setDeptNamePath(dept.getDeptNamePath());
                    outV.setTeamName(dept.getTeamName());
                    outV.setBranchName(dept.getBranchName());
                    outV.setGroupName(dept.getGroupName());
                }
                //5.统计时间
                outV.setStatisticsTime(statisticsTime);
                outK.setRoleType(1);
                outK.setStatisticsTime(statisticsTime);
                context.write(outK, outV);
            }
        } catch (Exception e) {
            System.out.println("彭钟调试PerformanceMapper line" + e.getMessage());
            System.out.println("彭钟调试PerformanceMapper line : " + line);
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, DimensionVO, EmployeePerformanceVO>.Context context) throws IOException, InterruptedException {
        try {
            HdfsUtil.close();
        } catch (Exception e) {
            System.out.println("PerformanceMapper close: " + e.getMessage());
        }
    }

}
