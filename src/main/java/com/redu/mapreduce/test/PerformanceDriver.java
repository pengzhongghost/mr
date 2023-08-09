package com.redu.mapreduce.test;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
public class PerformanceDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //hadoop jar wc.jar redu.mr.wordcount.WordcountDriver /user/joe/wordcount/input /user/joe/wordcount/output

        //String paidMonth = args[0];

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        configuration.set("paid_month", "2023-06");
        Job job = Job.getInstance(configuration);
        //job.setInputFormatClass(OrcInputFormat.class);

        // 2 设置jar加载路径
        job.setJarByClass(PerformanceDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(PerformanceMapper.class);
        //6. 设置combiner
        //job.setCombinerClass(PerformanceCombiner.class);
        job.setReducerClass(PerformanceReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(DimensionVO.class);
        job.setMapOutputValueClass(EmployeePerformanceVO.class);

        // 5 设置最终输出kv类型
        //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        //job.setOutputFormatClass(OrcOutputFormat.class);
        job.setOutputKeyClass(DimensionVO.class);
        job.setOutputValueClass(EmployeePerformanceVO.class);

        String ds = LocalDate.now().minusDays(1).format(DatePattern.PURE_DATE_FORMATTER);

        // 加载缓存数据
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_user/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/user_dept_origin/ds=" + ds + "/*"));
//        job.addCacheFile(new URI("file:///Users/pengzhong/Downloads/redu_user"));
//        job.addCacheFile(new URI("file:///Users/pengzhong/Downloads/user_dept_origin"));

        // 6 设置输入和输出路径
        //TextInputFormat.setInputPaths(job, new Path("/Users/pengzhong/Downloads/redu_order_tmp__a7368047_80e3_461d_9465_dc3a7dddc71d"));
        TextInputFormat.setInputPaths(job, new Path("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_order_uat/ds=" + ds + "/*"));
        //FileInputFormat.setInputPaths();
        //OrcInputFormat.setInputPaths(job, new Path("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_order/*"));
        //job.setInputFormatClass(OrcInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop001:9000/test/out/performance" + System.currentTimeMillis()));
        //FileOutputFormat.setOutputPath(job, new Path("/Users/pengzhong/Downloads/performance_temp" + System.currentTimeMillis()));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
