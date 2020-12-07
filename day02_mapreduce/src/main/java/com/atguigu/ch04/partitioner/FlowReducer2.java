package com.atguigu.ch04.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/2
 */
public class FlowReducer2 extends Reducer<Text, FlowBean2, Text, FlowBean2> {
    long sumUpFlow = 0;
    long sumDownFlow = 0;

    @Override
    protected void reduce(Text key, Iterable<FlowBean2> values, Context context) throws IOException, InterruptedException {
        for (FlowBean2 value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }
        FlowBean2 flowBean2 = new FlowBean2();
        flowBean2.set(sumUpFlow, sumDownFlow);
        context.write(key, flowBean2);
    }
}
