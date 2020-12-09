package com.atguigu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/9
 */
public class ZkClient {
    private ZooKeeper zooKeeper;

    @Before
    public void init() throws IOException {
        //Zookeeper链接地址
        String connect = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        //会话超出时间
        int sessionTimeout = 2000;
        //1.创建一个zookeeper对象
        zooKeeper = new ZooKeeper(connect,
                sessionTimeout,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
//                        System.out.println("调用开始了");
                    }
                });

    }

    @After
    public void destroy() throws InterruptedException {
        // 关闭资源
        zooKeeper.close();
    }

    /**
     *创建新节点
     */
    @Test
    public void testCreate() throws KeeperException, InterruptedException {
        zooKeeper.create("/testAPI",
                "123".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,  //访问控制列表，权限
                CreateMode.PERSISTENT); //创建的节点类型
    }

    /**
     * 测试查看
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void testLs() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/",
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println("自定义回调函数");
                    } //自定义回调函数，监听到根目录子节点变化会调用
                });

        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 获取值
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void get() throws KeeperException, InterruptedException {
        //节点状态（Stat结构体）
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData("/testAPI", false, stat);
        System.out.println(data.length);
        System.out.println(data);
        System.out.println(stat.getDataLength());
    }

    /**
     * 查询一个节点的状态
     */
    @Test
    public void testStat() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/testAPI", false);
        if(stat == null) {
            System.out.println("该节点不存在");
        } else {
            System.out.println(stat);
        }
    }

    /**
     * 赋值
     */
    @Test
    public void testSet() throws KeeperException, InterruptedException {
        String node = "/testAPI";
        Stat stat = zooKeeper.exists(node, false);

        //version目的之保证“我要修改的”和“我实际修改的”是同一个东西 乐观锁
        if(stat == null) {
            System.out.println("该节点不存在");
        }else {
            zooKeeper.setData(node, "abcd".getBytes(), stat.getVersion());
        }
    }

    /**
     * 删除
     */
    @Test
    public void testDelete() throws KeeperException, InterruptedException {
        zooKeeper.delete("/testAPI", 1);
    }
}
