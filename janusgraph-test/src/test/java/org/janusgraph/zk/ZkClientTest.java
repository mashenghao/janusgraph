package org.janusgraph.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author: mahao
 * @date: 2021/7/7
 */
public class ZkClientTest {


    private String connectString = "dn3:2181,dn4:2181,dn5:2181";
    private int sessionTimeout = 3000;
    ZooKeeper zkCli = null;

    // 初始化客户端
    @Before
    public void init() throws IOException {
        zkCli = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            // 回调监听
            @Override
            public void process(WatchedEvent event) {
                 System.out.println(event.getPath() + "\t" + event.getState() + "\t" + event.getType());
                try {
                    List<String> children = zkCli.getChildren("/", true);
                    for (String c : children) {
                        System.out.println(c);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // 创建子节点
    @Test
    public void createZnode() throws KeeperException, InterruptedException {
        String path = zkCli.create("/hello", "world".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
        Thread.sleep(3000);
    }

    // 获取子节点
    @Test
    public void getChild() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/", true);
        for (String c : children) {
            System.out.println(c);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    // 删除节点
    @Test
    public void rmChildData() throws KeeperException, InterruptedException {
        // byte[] data = zkCli.getData("/bbq", true, null);
        // System.out.println(new String(data));
        zkCli.delete("/hello", -1);
    }

    // 修改数据
    @Test
    public void setData() throws KeeperException, InterruptedException {
        zkCli.setData("/hello", "17".getBytes(), -1);
    }

    // 判断节点是否存在
    @Test
    public void testExist() throws KeeperException, InterruptedException {
        Stat exists = zkCli.exists("/hello", false);
        System.out.println(exists == null ? "not exists" : "exists");
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper("dn5:2181", 1000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.toString());
            }
        });
        List<String> children = zooKeeper.getChildren("/hbase", true);
        System.out.println(children);
    }
}

/**
 * ZooKeeper监听
 *
 * @author liuyuehu
 */
class MyZooKeeper {


    protected CountDownLatch countDownLatch = new CountDownLatch(1);
    public static ZooKeeper zooKeeper = null;


    private Object waiter = new Object();


    /**
     * 监控所有被触发的事件
     */
    public void process(WatchedEvent event) {
        System.out.println("收到事件通知：" + event.getState());
        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        }

    }


    /**
     * <p>连接Zookeeper</p>
     * 启动zk服务 本实例基于自动重连策略,如果zk连接没有建立成功或者在运行时断开,将会自动重连.
     */
    public void connect() {
        try {
            synchronized (waiter) {
                SessionWatcher watcher = new SessionWatcher();
                if (zooKeeper == null) {
                    // ZK客户端允许我们将ZK服务器的所有地址都配置在这里
                    zooKeeper = new ZooKeeper("dn3:2181", 1000, watcher);
                    // 使用CountDownLatch.await()的线程（当前线程）阻塞直到所有其它拥有
                    //CountDownLatch的线程执行完毕（countDown()结果为0）
                    countDownLatch.await();
                }
            }
        } catch (IOException e) {
            System.out.println("连接创建失败，发生 InterruptedException , e " + e.getMessage() + e);
        } catch (InterruptedException e) {
            System.out.println("连接创建失败，发生 IOException , e " + e.getMessage());
        }
        waiter.notifyAll();
    }

    /**
     * 关闭连接
     */
    public void close() {
        try {
            synchronized (waiter) {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                waiter.notifyAll();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class SessionWatcher implements Watcher {

        public void process(WatchedEvent event) {
            // 如果是“数据变更”事件
            if (event.getType() != Event.EventType.None) {
                return;
            }
            synchronized (waiter) {
                switch (event.getState()) {
                    case SyncConnected:
                        //zk连接建立成功,或者重连成功
                        waiter.notifyAll();
                        System.out.println("Connected...");
                        break;
                    case Expired:
                        // session过期,这是个非常严重的问题,有可能client端出现了问题,也有可能zk环境故障
                        // 此处仅仅是重新实例化zk client
                        System.out.println("Expired(重连)...");
                        connect();
                        break;
                    case Disconnected:
                        System.out.println("链接断开，或session迁移....");
                        break;
                    case AuthFailed:
                        close();
                        throw new RuntimeException("ZK Connection auth failed...");
                    default:
                        break;
                }
            }
        }
    }
}