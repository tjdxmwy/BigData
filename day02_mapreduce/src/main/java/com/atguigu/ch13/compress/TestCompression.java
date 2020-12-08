package com.atguigu.ch13.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.eclipse.jetty.util.IO;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/7
 */
public class TestCompression {
    public static void main(String[] args) throws IOException {
//        compress("e:/input2/month.txt", BZip2Codec.class); //压缩
        decompress("e:/input2/month.txt.bz2");
    }

    /**
     * 解压缩
     * @param file
     */
    private static void decompress(String file) throws IOException {
        Configuration configuration = new Configuration();
        //生成压缩文件格式工厂
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(configuration);
        //根据压缩格式工厂获取压缩对象
        CompressionCodec codec = codecFactory.getCodec(new Path(file));
        //输入流
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream fis = fs.open(new Path(file));
        CompressionInputStream cis = codec.createInputStream(fis);
        //输出流
        String outputFile = file.substring(0, file.length() - codec.getDefaultExtension().length());
        FSDataOutputStream fos = fs.create(new Path(outputFile));

        IOUtils.copyBytes(cis, fos, 1024);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fos);
    }

    /**
     * 压缩
     * @param file
     * @param codesClass
     */
    private static void compress(String file, Class<? extends CompressionCodec> codesClass) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        //生成压缩文件对象
        CompressionCodec codec = ReflectionUtils.newInstance(codesClass, configuration);
        //开输入流
        FSDataInputStream fis = fileSystem.open(new Path(file));
        //输出流
        FSDataOutputStream fos = fileSystem.create(new Path(file + codec.getDefaultExtension()));
        //用压缩格式包装输出流
        CompressionOutputStream cos = codec.createOutputStream(fos);
        IOUtils.copyBytes(fis, cos, 1024);
        //关流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(cos);
    }
}
