package com.atguigu.mapreduce.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author pengzhong
 * @since 2023/8/2
 */
public class PerformanceDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //hadoop jar wc.jar redu.mr.wordcount.WordcountDriver /user/joe/wordcount/input /user/joe/wordcount/output


        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
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
        job.setOutputKeyClass(DimensionVO.class);
        job.setOutputValueClass(EmployeePerformanceVO.class);

        // 加载缓存数据
        job.addCacheFile(new URI("/Users/pengzhong/Downloads/redu_user"));
        // Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/pengzhong/Downloads/redu_order_tmp__003e38a0_fe7f_4b85_b4b5_661715539fd2"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/pengzhong/Downloads/test" + System.currentTimeMillis()));
//        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_order/ds=20230804/*"));
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop001:9000/test/out/"+System.currentTimeMillis()));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
