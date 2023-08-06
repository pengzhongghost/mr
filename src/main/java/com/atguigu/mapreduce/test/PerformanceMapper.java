package com.atguigu.mapreduce.test;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcUtils;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
public class PerformanceMapper extends Mapper<LongWritable, Text, DimensionVO, EmployeePerformanceVO> {

    private DimensionVO outK = new DimensionVO();

    private EmployeePerformanceVO outV = new EmployeePerformanceVO();

    private final Map<String, String> userMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取缓存的文件，并把文件内容封装到集合
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Reader reader = OrcFile.createReader(new Path(cacheFiles[0]), new OrcFile.ReaderOptions(new Configuration()).filesystem(fs));
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        while (null != batch) {

        }
        // 从流中读取数据
        //BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
//        while (StringUtils.isNotEmpty(line = reader.readLine())) {
//            // 切割
//            String[] fields = line.split("\t");
//
//            // 赋值
//            userMap.put(fields[0], fields[1]);
//        }

        // 关流
        IOUtils.closeStream(reader);
    }



    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DimensionVO, EmployeePerformanceVO>.Context context) throws IOException, InterruptedException {
        outV = new EmployeePerformanceVO();
        String line = value.toString();
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
            String partnerId = split[46];
            //String partnerName = split[135];
            String partnerDeptIdPath = split[48];
            String partnerDeptNamePath = split[49];
            String paidTime = split[5];
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
            //outV.setEmployeeName(partnerName);
            outV.setOrderCount(1);
            //FIXME 读取另外一张表
            //outV.setEmployeeNo();
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
            String statisticsTime = DateUtil.format(DateUtil.parse(paidTime, DatePattern.NORM_DATETIME_FORMAT), DatePattern.NORM_MONTH_FORMATTER);
            outV.setStatisticsTime(statisticsTime);
            outK.setPlatform(platformCode);
            outK.setRoleType(1);
            outK.setEmployeeNo("0");
            outK.setStatisticsTime(statisticsTime);
            //outV.setEmployeeNo();
            context.write(outK, outV);
        }
    }
}
