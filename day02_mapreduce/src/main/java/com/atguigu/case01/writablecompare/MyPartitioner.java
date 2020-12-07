package com.atguigu.case01.writablecompare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/4
 */
public class MyPartitioner extends Partitioner<FlowBean, Text> {


    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        int partitionNum = 4;
        String prePhoneNum = text.toString().substring(0, 3);

        // 2 根据手机号归属地设置分区
        if("136".equals(prePhoneNum)) {
            partitionNum = 0;
        }else if("137".equals(prePhoneNum)) {
            partitionNum = 1;
        }else if("138".equals(prePhoneNum)) {
            partitionNum = 2;
        }else if("139".equals(prePhoneNum)) {
            partitionNum = 3;
        }

        return partitionNum;
    }
}
