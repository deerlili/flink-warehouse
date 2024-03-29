package com.deerlili.gmall.realtime.utils;

import lombok.SneakyThrows;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * ThreadPoolTest 测试线程池
 *
 * @author lixx
 * @date 2022/8/1 15:10
 */
public class ThreadPoolTest {
    public static void main(String[] args) {
        ThreadPoolExecutor threadPool = ThreadPoolUtil.getThreadPool();

        for (int i = 0; i < 10; i++) {
            threadPool.submit(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName() + "-dd");
                    Thread.sleep(2000);
                }
            });
        }
    }
}
