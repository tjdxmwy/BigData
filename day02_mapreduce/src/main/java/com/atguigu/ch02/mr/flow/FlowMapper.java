package com.atguigu.ch02.mr.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/1
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();
    Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取一行
        String line = value.toString();
        //2.切割
        String[] words = line.split("\t");
        //3.封装对象
        //3.1.取出手机号
        String phoneNum = words[1];
        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(words[words.length - 3]);
        long downFlow = Long.parseLong(words[words.length - 2]);
        flowBean.set(upFlow, downFlow);
        word.set(phoneNum);
        context.write(word, flowBean);
    }
}
