package com.atguigu.ch06.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/3
 */
public class WcCombinerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WcCombinerDriver.class);
        job.setMapperClass(WcCombinerMapper.class);
        job.setReducerClass(WcCombinerReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:/input2"));
        FileOutputFormat.setOutputPath(job, new Path("E:/output2"));

        job.setCombinerClass(WcCombinerReducer.class); // 加了Combiner后，时间从17->8

        boolean b = job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        System.out.println("一共用时： "+ (end - start) / 1000); // 一共用时： 17
        System.exit(b?0:1);
    }
}
