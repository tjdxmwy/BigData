package com.atguigu.ch04.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/2
 */
public class FlowMapper2 extends Mapper<LongWritable, Text, Text, FlowBean2> {

    FlowBean2 flow = new FlowBean2();
    Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取一行
        String line = value.toString();
        String[] split = line.split("\t");
        String phoneNum = split[1];
        flow.set(Long.parseLong(split[split.length-3]), Long.parseLong(split[split.length-2]));
        word.set(phoneNum);
        context.write(word, flow);
    }
}
