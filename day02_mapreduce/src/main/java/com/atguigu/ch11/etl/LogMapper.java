package com.atguigu.ch11.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/6
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Counter pass;
    private Counter fail;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        pass = context.getCounter("ETL", "pass");
//        fail = context.getCounter("ETL", "fail");
        pass = context.getCounter(ETL.PASS);
        fail = context.getCounter(ETL.FAIL);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(" ");
        if(splits.length > 11) {
            pass.increment(1);
            context.write(value, NullWritable.get());
        }else {
            fail.increment(1);
        }
    }
}
