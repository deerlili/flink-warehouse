package com.deerlili.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.common.HbaseConfig;
import com.deerlili.gmall.realtime.utils.DimUtil;
import com.deerlili.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * DimAsyncFunction 外部数据访问的异步
 *
 * @author lixx
 * @date 2022/8/1 14:49
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    public abstract String getKey(T input);

    public abstract void join(T input,JSONObject dimInfo);

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化连接
        Class.forName(HbaseConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(HbaseConfig.PHOENIX_SERVER);
        //初始化线程池
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //获取查询主键
                    String id = getKey(input);
                    //查询维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName , id);
                    //补充维度信息
                    if (dimInfo != null) {
                        join(input,dimInfo);
                    }
                    //数据输出
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        //生产环境超时后再发起一次请求
        //这里直接输出
        System.out.println("TimeOut:"+input);
    }
}
