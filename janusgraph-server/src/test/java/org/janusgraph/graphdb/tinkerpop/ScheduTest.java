package org.janusgraph.graphdb.tinkerpop;

import org.janusgraph.diskstorage.log.Log;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author: mahao
 * @date: 2021/6/29
 */
public class ScheduTest {

    static long i = 0;

    public static void main(String[] args) throws InterruptedException {
        final ScheduledExecutorService bindExecutor = Executors.newScheduledThreadPool(1);
        bindExecutor.scheduleWithFixedDelay(() -> {
            System.out.println(String.valueOf(i++));
            if (i == 5) {
                System.out.println("抛出异常");
                throw new RuntimeException("抛出错误");
            }
        }, 0, 1, TimeUnit.SECONDS);

       bindExecutor.awaitTermination(10,TimeUnit.SECONDS);
        System.out.println("mian end");
    }
}
