package com.deerlili.gmall.realtime.utils;

import com.deerlili.gmall.realtime.bean.TransientSink;
import com.deerlili.gmall.realtime.common.HbaseConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClickHouseUtil clickhouse util
 *
 * @author lixx
 * @date 2022/8/9 9:06
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getClickHouseSink(String sql) {
        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                try {
                    //获取所有的属性信息
                    Field[] fields = t.getClass().getDeclaredFields();
                    //遍历字段
                    int offset = 0;
                    for (int i = 0; i < fields.length; i++) {
                        //获取字段
                        Field field = fields[i];
                        //设置私有属性可访问
                        field.setAccessible(true);
                        //获取字段上的注解，是否参与序列化
                        TransientSink annotation = field.getAnnotation(TransientSink.class);
                        if (annotation != null) {
                            //存在该注解
                            offset++;
                            continue;
                        }
                        //获取值(反射)
                        Object value = field.get(t);
                        //给预编译SQL赋值
                        preparedStatement.setObject(i + 1 - offset, value);
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        , new JdbcExecutionOptions.Builder().withBatchSize(5).build()
        , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(HbaseConfig.CLICKHOUSE_DRIVER)
                        .withUrl(HbaseConfig.CLICKHOUSE_URL)
                        .build());
    }

}
