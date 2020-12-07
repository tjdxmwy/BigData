package com.atguigu.ch05.compare;

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
public class FlowCountMapper extends Mapper<LongWritable, Text, FlowCountBean, Text> {
    private Text word = new Text();
    private FlowCountBean flowCountBean = new FlowCountBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        word.set(splits[0]);
        flowCountBean.setUpFlow(Long.parseLong(splits[1]));
        flowCountBean.setDownFlow(Long.parseLong(splits[2]));
        flowCountBean.setSumFlow(Long.parseLong(splits[3]));
        context.write(flowCountBean, word);
    }
}
