package com.atguigu.ch04.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/2
 */
public class MyPartitioner extends Partitioner<Text, FlowBean2> {
    @Override
    public int getPartition(Text text, FlowBean2 flowBean2, int numPartitions) {
        String headNum = text.toString().substring(0, 3);
        switch (headNum) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
