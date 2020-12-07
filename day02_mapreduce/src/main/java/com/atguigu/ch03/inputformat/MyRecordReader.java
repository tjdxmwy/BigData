package com.atguigu.ch03.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description 负责将文件转换成key value对
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/2
 */
public class MyRecordReader extends RecordReader<Text, BytesWritable> {

    //表示文件读完了，默认是false，表示文件没读过
    private boolean isRead = false;
    // kv pair
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FileSplit fs;
    private FSDataInputStream inputStream;

    /**
     * 初始化方法
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 开流
        fs = (FileSplit) split;
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        inputStream = fileSystem.open(fs.getPath());
    }

    /**
     * 读取下一组 key, value pair.
     * @return 返回true说明已经key value读完了
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!isRead) {
            //读取这个文件，用流
            //填充key
            String name = fs.getPath().getName();
            key.set(name);
            //填充value
            byte[] buffer = new byte[(int) fs.getLength()];
            inputStream.read(buffer);
            value.set(buffer, 0, buffer.length);
            //标记为已读状态
            isRead = true;

            return true;
        } else {
            return false;
        }
    }


    /**
     * 读取当前的key
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 读取当前的value
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 读取进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isRead ? 1 : 0;
    }

    /**
     * 关闭方法，一般用来关闭资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        //关流
        IOUtils.closeStream(inputStream);
    }
}
