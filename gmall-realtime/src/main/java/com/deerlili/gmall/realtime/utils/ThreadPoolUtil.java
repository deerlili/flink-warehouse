package com.deerlili.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ThreadPoolUtil 线程池 双重校验,防止多次请求多次创建
 *
 * @author lixx
 * @date 2022/8/1 14:55
 */
public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {

    }

    public static ThreadPoolExecutor getThreadPool() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolExecutor.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            8, 16, 60L, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
