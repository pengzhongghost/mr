package com.redu.mapreduce.test;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.redu.mapreduce.util.HdfsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;
import java.net.URI;
import java.util.*;

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
    private final Map<String, EmployeeVO> userMap = new HashMap<>();

    private final Map<String, String> userDeptOriginMap = new HashMap<>();

    private String paidMonth;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1.获取redu_user表中的相关信息
        // 获取缓存的文件，并把文件内容封装到集合
        URI[] cacheFiles = context.getCacheFiles();
        URI uri01 = cacheFiles[0];
        String dirName01 = uri01.toString().split("/\\*")[0];
        List<Path> paths = HdfsUtil.ls(dirName01);
        for (Path path : paths) {
            Reader userReader = OrcFile.createReader(path, OrcFile.readerOptions(context.getConfiguration()));
            // 解析schema
            VectorizedRowBatch userInBatch = userReader.getSchema().createRowBatch();
            // 流解析文件
            //1)user表
            RecordReader rows = userReader.rows();
            while (rows.nextBatch(userInBatch)) {   // 读1个batch
                // 列式读取
                LongColumnVector userId = (LongColumnVector) userInBatch.cols[0];
                BytesColumnVector name = (BytesColumnVector) userInBatch.cols[2];
                BytesColumnVector employeeNo = (BytesColumnVector) userInBatch.cols[8];
                for (int i = 0; i < userInBatch.size; i++) {
                    // 注意：因为是列存储，所以name列是一个大buffer存储的，需要从里面的start偏移量取length长度的才是该行的列值
                    EmployeeVO employee = EmployeeVO.builder().userId(String.valueOf(userId.vector[i]))
                            .employeeNo(new String(employeeNo.vector[i], employeeNo.start[i], employeeNo.length[i]))
                            .name(new String(name.vector[i], name.start[i], name.length[i])).build();
                    userMap.put(employee.getUserId(), employee);
                }
            }
            rows.close();
            // 关流
            IOUtils.closeStream(userReader);
        }
        //2.获取user_dept_origin中的信息
        URI uri02 = cacheFiles[0];
        String dirName02 = uri02.toString().split("/\\*")[0];
        List<Path> path02s = HdfsUtil.ls(dirName02);
        for (Path path02 : path02s) {
            Reader userDeptOriginReader = OrcFile.createReader(path02, OrcFile.readerOptions(context.getConfiguration()));
            VectorizedRowBatch userDeptOriginBatch = userDeptOriginReader.getSchema().createRowBatch();
            //2)userDeptOrigin
            RecordReader userDeptOriginrows = userDeptOriginReader.rows();
            while (userDeptOriginrows.nextBatch(userDeptOriginBatch)) {   // 读1个batch
                // 列式读取
                for (int i = 0; i < userDeptOriginBatch.size; i++) {
                    String fid = String.valueOf(((LongColumnVector) userDeptOriginBatch.cols[1]).vector[i]);
                    String reduId = String.valueOf(((LongColumnVector) userDeptOriginBatch.cols[3]).vector[i]);
                    BytesColumnVector typeColumn = (BytesColumnVector) userDeptOriginBatch.cols[2];
                    String type = new String(typeColumn.vector[i], typeColumn.start[i], typeColumn.length[i]);
                    BytesColumnVector fromTableColum = (BytesColumnVector) userDeptOriginBatch.cols[5];
                    String fromTable = new String(fromTableColum.vector[i], fromTableColum.start[i], fromTableColum.length[i]);
                    userDeptOriginMap.put(type + "|" + fromTable + "|" + fid, reduId);
                }
            }
            userDeptOriginrows.close();
            // 关流
            IOUtils.closeStream(userDeptOriginReader);
        }
        //2.付款时间
        paidMonth = context.getConfiguration().get("paid_month");
    }


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DimensionVO, EmployeePerformanceVO>.Context context) throws IOException, InterruptedException {
        outV = new EmployeePerformanceVO();
        String line = value.toString();
        try {
            String[] split = line.split("\u0001");
            String platformCode = split[1];
            String appletPeg = split[3];
            List<String> appletList = Arrays.asList(appletPeg.split(","));
            if (("1".equals(platformCode) && appletList.contains("1"))
                    || ("2".equals(platformCode) && appletList.contains("0"))
                    || "4".equals(platformCode)) {
                String orderStatus = split[2];
                String estimateSettlementAmount = split[24];
                String finalServiceIncome = split[56];
                String achievementsArderMultiple = split[37];
                String estimateServiceIncome = split[27];
                //String channelId = split[50];
                OrderExtVO orderExt = JSONUtil.toBean(split[62], OrderExtVO.class);
                String partnerId = userDeptOriginMap.get("USER|PARTNER|" + orderExt.getHiPartnerid());
                //String partnerName = split[135];
                String partnerDeptIdPath = split[48];
                String partnerDeptNamePath = split[49];
                String paidTime = split[5];
                if (StrUtil.isEmpty(partnerId) || "0".equals(partnerId)) {
                    return;
                }
                String statisticsTime = DateUtil.format(DateUtil.parse(paidTime, DatePattern.NORM_DATETIME_FORMAT), DatePattern.NORM_MONTH_FORMATTER);
                if (!paidMonth.equals(statisticsTime)) {
                    return;
                }
                //1.平台
                if (StrUtil.isEmpty(platformCode)) {
                    outV.setPlatform("-");
                }
                switch (platformCode) {
                    case "1":
                        outV.setPlatform("dy");
                        break;
                    case "2":
                        outV.setPlatform("ks");
                        break;
                    case "4":
                        outV.setPlatform("wx");
                        break;
                    default:
                        outV.setPlatform("-");
                }
                //2.订单数量和gmv
                if ("4".equals(orderStatus)) {
                    outV.setFundOrderCount(1);
                    outV.setFundOrderGmv(estimateSettlementAmount);
                } else {
                    outV.setValidOrderNum(1);
                    outV.setValidServiceIncome(finalServiceIncome);
                    outV.setOrderAchievementSum(achievementsArderMultiple);
                }
                outV.setGmv(estimateSettlementAmount);
                outV.setEstimateServiceIncome(estimateServiceIncome);
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
                } else {
                    outK.setEmployeeNo("0");
                }
                //4.部门信息
                outV.setDeptIdPath(partnerDeptIdPath);
                String[] partnerIds = partnerDeptIdPath.split("/");
                if (partnerIds.length >= 5) {
                    outV.setTeamId(Integer.parseInt(partnerIds[2]));
                    outV.setBranchId(Integer.parseInt(partnerIds[3]));
                    outV.setGroupId(Integer.parseInt(partnerIds[4]));
                }
                outV.setDeptNamePath(partnerDeptNamePath);
                String[] partnerNames = partnerDeptNamePath.split("/");
                if (partnerNames.length >= 5) {
                    outV.setTeamName(partnerNames[2]);
                    outV.setBranchName(partnerNames[3]);
                    outV.setGroupName(partnerNames[4]);
                }
                //5.统计时间
                outV.setStatisticsTime(statisticsTime);
                outK.setPlatform(platformCode);
                outK.setRoleType(1);
                outK.setStatisticsTime(statisticsTime);
                context.write(outK, outV);
            }
        } catch (Exception e) {
            System.out.println("彭钟调试PerformanceMapper line" + e.getMessage());
            System.out.println("彭钟调试PerformanceMapper line : " + line);
        }
    }

}
