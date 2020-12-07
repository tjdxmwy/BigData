package com.atguigu.ch08.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/4
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    FSDataOutputStream atguigu;
    FSDataOutputStream other;


    public MyRecordWriter(TaskAttemptContext job) throws IOException {
        Configuration configuration = job.getConfiguration();
        String outDir = configuration.get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(configuration);
        atguigu = fileSystem.create(new Path(outDir + "/atguigu.log"));
        other = fileSystem.create(new Path(outDir + "/other.log"));

    }

    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String line = value.toString() + "\n";
        if(line.contains("atguigu")) {
            atguigu.write(line.getBytes());
        }else {
            other.write(line.getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
