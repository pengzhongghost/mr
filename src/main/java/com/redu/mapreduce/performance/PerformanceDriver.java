package com.redu.mapreduce.performance;

import cn.hutool.core.date.DatePattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
public class PerformanceDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        String sourceTable = args[0];
        String targetTable = args[1];
        configuration.set("paid_month", args[2]);
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(PerformanceDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(PerformanceMapper.class);
        job.setReducerClass(PerformanceReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(DimensionVO.class);
        job.setMapOutputValueClass(EmployeePerformanceVO.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(EmployeePerformanceVO.class);

        String ds = LocalDate.now().minusDays(1).format(DatePattern.PURE_DATE_FORMATTER);

        // 加载缓存数据
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_user/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/user_dept_origin/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_dept/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/config/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/config_item/ds=" + ds + "/*"));

        // 6 设置输入和输出路径
        TextInputFormat.setInputPaths(job, new Path("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/" + sourceTable + "/ds=20230808/*"));

        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/" + targetTable));
        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
