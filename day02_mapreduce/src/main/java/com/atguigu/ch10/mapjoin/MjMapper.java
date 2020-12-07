package com.atguigu.ch10.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/6
 */
public class MjMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text text = new Text();
    private HashMap<String, String> hMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream pd = fileSystem.open(new Path(cacheFiles[0]));
        BufferedReader br = new BufferedReader(new InputStreamReader(pd));

        String line;
        while(StringUtils.isNotEmpty(line = br.readLine())) {
            String[] split = line.split("\t");
            hMap.put(split[0], split[1]);
        }
        IOUtils.closeStream(br);
    }

    /**
     * 处理order.txt的文件
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        text.set(splits[0] + "\t" + hMap.get(splits[1]) + "\t" + splits[2]);
        context.write(text, NullWritable.get());
    }
}
