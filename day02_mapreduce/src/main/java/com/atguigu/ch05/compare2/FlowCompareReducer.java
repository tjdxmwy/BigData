package com.atguigu.ch05.compare2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/3
 */
public class FlowCompareReducer extends Reducer<FlowCompareBean, Text, Text, FlowCompareBean> {
    @Override
    protected void reduce(FlowCompareBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
