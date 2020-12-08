package com.atguigu.ch14.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/8
 */
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        flowBean.setPhone(splits[0]);
        flowBean.set(Long.parseLong(splits[1]), Long.parseLong(splits[2]));
        context.write(flowBean, NullWritable.get());
    }
}
