package com.redu.mapreduce.per;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author pengzhong
 * @since 2023/8/3
 */
public class PerformanceCombiner extends Reducer<LongWritable, EmployeePerformanceVO, Text, EmployeePerformanceVO>  {

    @Override
    protected void reduce(LongWritable key, Iterable<EmployeePerformanceVO> values, Reducer<LongWritable, EmployeePerformanceVO, Text, EmployeePerformanceVO>.Context context) throws IOException, InterruptedException {
        System.out.println(key);
    }

}
