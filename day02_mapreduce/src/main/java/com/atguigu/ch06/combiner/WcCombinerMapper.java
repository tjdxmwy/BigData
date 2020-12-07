package com.atguigu.ch06.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/3
 */
public class WcCombinerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text words = new Text();
    private IntWritable wordNum = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split(" ");
        for (String split : splits) {
            words.set(split);
            context.write(words, wordNum);
        }

    }
}
