package com.atguigu.ch05.compare2;

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
public class FlowCompareMapper extends Mapper<LongWritable, Text, FlowCompareBean, Text> {
    private Text phone = new Text();
    private FlowCompareBean compareBean = new FlowCompareBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        phone.set(splits[0]);
        compareBean.setUpFlow(Long.parseLong(splits[1]));
        compareBean.setDownFlow(Long.parseLong(splits[2]));
        compareBean.setSumFlow(Long.parseLong(splits[3]));
        context.write(compareBean, phone);
    }
}
