package com.atguigu.ch05.compare2;

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
 * @Date 2020/12/3
 */
public class FlowCompareDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(FlowCompareDriver.class);

        job.setMapperClass(FlowCompareMapper.class);
        job.setReducerClass(FlowCompareReducer.class);

        job.setMapOutputKeyClass(FlowCompareBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowCompareBean.class);

        job.setSortComparatorClass(FlowCompartor.class);

        FileInputFormat.setInputPaths(job, new Path("e:/output"));
        FileOutputFormat.setOutputPath(job, new Path("e:/output2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
