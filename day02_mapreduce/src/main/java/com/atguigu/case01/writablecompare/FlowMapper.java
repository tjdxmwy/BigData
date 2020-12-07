package com.atguigu.case01.writablecompare;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/4
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    long upFlow;
    long downFlow;
    FlowBean flowBean = new FlowBean();
    Text phoneNum = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String[] split = value.toString().split("\t");
        //2.封装
        upFlow = Long.parseLong(split[1]);
        downFlow = Long.parseLong(split[2]);
        flowBean.set(upFlow, downFlow);
        phoneNum.set(split[0]);
        //3.输出
        context.write(flowBean, phoneNum);
    }
}
