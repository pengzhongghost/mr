package com.redu.mapreduce.channel;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.redu.mapreduce.test.*;
import com.redu.mapreduce.util.HdfsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            URI uri02 = cacheFiles[1];
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
                        userDeptOriginMap.put(fromTable + "|" + fid, reduId);
                        if ("DEPT".equals(type) && "GROUP".equals(fromTable)) {
                            groupIdMap.put(reduId, type + "|" + fromTable + "|" + fid);
                        }
                    }
                }
                userDeptOriginrows.close();
                // 关流
                IOUtils.closeStream(userDeptOriginReader);
            }
            //3.获取user表中的信息
            URI uri03 = cacheFiles[2];
            String dirName03 = uri03.toString().split("/\\*")[0];
            List<Path> path03s = HdfsUtil.ls(dirName03);
            for (Path path03 : path03s) {
                Reader deptReader = OrcFile.createReader(path03, OrcFile.readerOptions(context.getConfiguration()));
                VectorizedRowBatch deptBatch = deptReader.getSchema().createRowBatch();
                //3)dept
                RecordReader deptRows = deptReader.rows();
                while (deptRows.nextBatch(deptBatch)) {   // 读1个batch
                    // 列式读取
                    for (int i = 0; i < deptBatch.size; i++) {
                        //从dept中获取
                        String id = String.valueOf(((LongColumnVector) deptBatch.cols[0]).vector[i]);
                        String key = groupIdMap.get(id);
                        if (StrUtil.isNotEmpty(key)) {
                            //idPath,namePath
                            BytesColumnVector idPathColumn = (BytesColumnVector) deptBatch.cols[4];
                            String idPath = new String(idPathColumn.vector[i], idPathColumn.start[i], idPathColumn.length[i]);
                            String[] idSplit = idPath.split("/");
                            BytesColumnVector namePathColumn = (BytesColumnVector) deptBatch.cols[5];
                            String namePath = new String(namePathColumn.vector[i], namePathColumn.start[i], namePathColumn.length[i]);
                            String[] nameSplit = namePath.split("/");
                            DeptVO dept = new DeptVO();
                            if (idSplit.length > 2) {
                                dept.setTeamId(Integer.parseInt(idSplit[2]));
                                dept.setTeamName(nameSplit[2]);
                                if (idSplit.length > 3) {
                                    dept.setBranchId(Integer.parseInt(idSplit[3]));
                                    dept.setBranchName(nameSplit[3]);
                                    if (idSplit.length > 4) {
                                        dept.setGroupId(Integer.parseInt(idSplit[4]));
                                        dept.setGroupName(nameSplit[4]);
                                    }
                                }
                            }
                            dept.setDeptIdPath(idPath);
                            dept.setDeptNamePath(namePath);
                            deptMap.put(key, dept);
                        }
                    }
                }
                deptRows.close();
                // 关流
                IOUtils.closeStream(deptReader);
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
            String platformCode = split[1];
            String appletPeg = split[3];
            List<String> appletList = Arrays.asList(appletPeg.split(","));
            if (("1".equals(platformCode) && appletList.contains("1"))
                    || ("2".equals(platformCode) && appletList.contains("0"))
                    || "4".equals(platformCode)) {
                String orderStatus = split[2];
                String estimateSettlementAmount = split[24];
                String achievementsArderMultiple = split[37];
                String serviceFeeRate = split[26];
                String estimateServiceIncome = split[27];
                //String channelId = split[50];
                OrderExtVO orderExt = JSONUtil.toBean(split[62], OrderExtVO.class);
                String channelId = userDeptOriginMap.get("CHANNEL|" + orderExt.getHiChannelid());
                //String partnerName = split[135];
                String paidTime = split[5];
                if (StrUtil.isEmpty(channelId) || "0".equals(channelId)) {
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
                    outV.setValidServiceIncome(orderExt.getChannelFinalServiceIncome());
                    outV.setOrderAchievementSum(achievementsArderMultiple);
                }
                outV.setGmv(estimateSettlementAmount);
                outV.setEstimateServiceIncome(estimateServiceIncome);
                //3.渠道
                if (NumberUtil.isNumber(channelId)) {
                    outV.setUserId(Long.parseLong(channelId));
                    outK.setUserId(Long.parseLong(channelId));
                } else {
                    outK.setUserId(0);
                }
                outV.setRoleType(2);
                outV.setOrderCount(1);
                //工号
                EmployeeVO employee = userMap.get(channelId);
                if (null != employee) {
                    outV.setEmployeeNo(employee.getEmployeeNo());
                    outV.setEmployeeName(employee.getName());
                    outK.setEmployeeNo(employee.getEmployeeNo());
                } else {
                    outK.setEmployeeNo("0");
                }
                //4.部门信息
                DeptVO dept = deptMap.get("DEPT|GROUP|" + orderExt.getChannelGroupId());
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
                outK.setPlatform(platformCode);
                outK.setRoleType(2);
                outK.setStatisticsTime(statisticsTime);
                outK.setPaidTime(DateUtil.parse(paidTime, DatePattern.NORM_DATETIME_FORMAT).getTime());
                outV.setPaidTimeStr(paidTime);
                outV.setServiceFeeRate(serviceFeeRate);
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
