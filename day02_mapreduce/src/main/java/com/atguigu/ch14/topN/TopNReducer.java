package com.atguigu.ch14.topN;

import jdk.nashorn.api.scripting.URLReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/8
 */
public class TopNReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        for(int i=0; i<10; i++) {
            if(iterator.hasNext()) {
                context.write(key, iterator.next());
            }
        }
    }
}
