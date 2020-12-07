package com.atguigu.ch03.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Description 返回一个自定义的RecordReader
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/2
 */

/**
 * 需求：
 *      将多个小文件合并成一个SequenceFile文件
 *      （SequenceFile文件是Hadoop用来存储二进制形式的key-value对的文件格式），
 *      SequenceFile里面存储着多个文件，
 *      存储的形式为文件路径+名称为key，文件内容为value。
 */
public class MyInputFormat extends FileInputFormat<Text, BytesWritable> {

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MyRecordReader();
    }
}
