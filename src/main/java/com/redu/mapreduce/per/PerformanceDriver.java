package com.redu.mapreduce.per;

import cn.hutool.core.date.DatePattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

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
        configuration.set("paid_month", LocalDate.now().minusMonths(1).format(DatePattern.NORM_MONTH_FORMATTER));

        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(configuration,"struct<team_name:string,team_id:int,branch_name:string,branch_id:int,group_name:string,group_id:int,dept_id_path:string,dept_name_path:string,employee_name:string,statistics_time:string,platform:string,order_count:bigint,fund_order_count:bigint,valid_order_num:bigint,gmv:string,fund_order_gmv:string,valid_service_income:string,role_type:int,employee_no:string,order_achievement_sum:string,valid_order_achievement_sum:string,estimate_service_income:string,user_id:bigint,performance_new:string,ds:string,hired_date:string,is_formal:string>");

        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(PerformanceDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(PerformanceMapper.class);
        //6. 设置combiner
        job.setReducerClass(PerformanceReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(DimensionVO.class);
        job.setMapOutputValueClass(EmployeePerformanceVO.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);

        String ds = LocalDate.now().minusDays(1).format(DatePattern.PURE_DATE_FORMATTER);

        // 加载缓存数据
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_user/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/user_dept_origin/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_dept/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/ding_employee/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/config/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/config_item/ds=" + ds + "/*"));
        job.addCacheFile(new URI("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/dept_user_role/ds=" + ds + "/*"));

        // 6 设置输入和输出路径
        //TextInputFormat.setInputPaths(job, new Path("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_order_uat/ds=20230818"));
        TextInputFormat.setInputPaths(job, new Path("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/redu_order/ds=" + ds +"/*"));
        job.setOutputFormatClass(OrcOutputFormat.class);
        //OrcOutputFormat.setOutputPath(job, new Path("hdfs://hadoop001:9000/test/out/performance_temp/" + System.currentTimeMillis()));
        OrcOutputFormat.setOutputPath(job, new Path("hdfs://hadoop001:9000/user/hive/warehouse/data_cube.db/performance_temp/ds=" + ds));
        //第一次排序的
        //job.setSortComparatorClass(MapOutValueComparator.class);
        //FIXME 第二次分组排序的
        //job.setGroupingComparatorClass(MapOutValueComparator.class);
        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
