package com.atguigu.ch10.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/6
 */
public class MjDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MjDriver.class);
        job.setMapperClass(MjMapper.class);
        job.setNumReduceTasks(0);

        //添加分布式缓存
        job.addCacheFile(URI.create("file:///e:/input2/pd.txt"));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("e:/input2/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("e:/output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
