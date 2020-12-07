package com.atguigu.ch04.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * @Date 2020/12/2
 */
public class FlowDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job实例
        Job job = Job.getInstance(new Configuration());
        //2.指定本程序jar包地址
        job.setJarByClass(FlowDriver2.class);
        //3.指定本业务job要使用的mapper的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean2.class);
        //4.指定本程序中mapper和reduce类
        job.setMapperClass(FlowMapper2.class);
        job.setReducerClass(FlowReducer2.class);
        //5.设置切片任务和切片个数
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(5);
        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:/input"));
        FileOutputFormat.setOutputPath(job, new Path("E:/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);


    }
}
