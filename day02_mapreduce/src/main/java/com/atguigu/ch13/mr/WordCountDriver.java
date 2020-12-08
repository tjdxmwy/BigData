package com.atguigu.ch13.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/6
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop102:8020");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("mapreduce.app-submission.cross-platform", "true");//是否允许从windows向linux提交
        configuration.set("yarn.resourcemanager.hostname", "hadoop103");

        //在shuffle端执行
        configuration.setBoolean("mapreduce.map.output.compress", true);
        configuration.setClass("mapreduce.map.output.compress.codec",
                GzipCodec.class,
                CompressionCodec.class);

        Job job = Job.getInstance(configuration);
        //        job.setJarByClass(WcDriver.class);
        job.setJar("E:\\BigData\\note\\03_Hadoop\\Code\\day02_mapreduce\\target\\day02_mapreduce-1.0-SNAPSHOT.jar");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean b = job.waitForCompletion(true);

        long end = System.currentTimeMillis();
        System.out.println(end - start);
        System.exit(b?0:1);

    }
}
