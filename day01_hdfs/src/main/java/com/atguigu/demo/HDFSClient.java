package com.atguigu.demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/11/28
 */
public class HDFSClient {
    FileSystem fileSystem;

    @Before
    public void init() throws IOException, InterruptedException {
        fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:8020"),
                new Configuration(), "atguigu");
    }

    @After
    public void destory() throws IOException {
        fileSystem.close();
    }

    /**
     * 上传
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void put() throws IOException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        configuration.set("dfs.blocksize", "67108864");
        //1. 新建HDFS对象
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:8020"),
                configuration, "atguigu");
        //2. 操作集群
        fileSystem.copyFromLocalFile(new Path("E:\\大数据开发\\04.Hadoop\\代码\\Hadoop\\2mapreduce200105(1).zip"),
                new Path("/1.zip"));
        //3. 关闭资源
        fileSystem.close();
    }

    /**
     * 下载
     * @throws IOException
     */
    @Test
    public void get() throws IOException {
        fileSystem.copyToLocalFile(new Path("/1.zip"),
                new Path("e:\\2.zip"));
    }

    /**
     * 追加
     * @throws IOException
     */
    @Test
    public void append() throws IOException {
        FSDataOutputStream append = fileSystem.append(new Path("/NOTICE.txt"));
        append.write("TestAPI".getBytes());
        IOUtils.closeStream(append);
    }

    /**
     * 查看
     * @throws IOException
     */
    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getGroup());
            System.out.println("========================");
        }
    }

    @Test
    public void lf() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFile = fileSystem.listFiles(new Path("/"), true);
        while(locatedFile.hasNext()) {
            LocatedFileStatus locatedFileStatus = locatedFile.next();
            System.out.println(locatedFileStatus.getPath());
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for(int i=0; i<blockLocations.length; i++) {
                System.out.println("第" + i + "块");
                String[] hosts = blockLocations[i].getHosts();
                for (String host : hosts) {
                    System.out.print(host);
                }
                System.out.println();
            }
            System.out.println("==================");
        }
    }

    /**
     * 移动
     */
    @Test
    public void mv() throws IOException {
        fileSystem.rename(new Path("/1.tar.gz"),
                new Path("/logs/2.tar.gz"));
    }
}
