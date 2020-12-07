package com.atguigu.ch05.compare2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/3
 */
public class FlowCompartor extends WritableComparator {

    protected FlowCompartor() {
        super(FlowCompareBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowCompareBean fa = (FlowCompareBean) a;
        FlowCompareBean fb = (FlowCompareBean) b;
        return Long.compare(fb.getSumFlow(), fa.getSumFlow());
    }
}
