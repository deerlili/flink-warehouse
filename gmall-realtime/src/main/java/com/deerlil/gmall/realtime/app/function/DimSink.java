package com.deerlil.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.deerlil.gmall.realtime.common.HbaseConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.util.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author lixx
 * @date 2022/6/13
 * @notes hbase create dim table
 */
@Slf4j
public class DimSink extends RichSinkFunction<JSONObject> {
    private static final long serialVersionUID = 1905122041950251207L;

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化Phoenix连接
        Class.forName(HbaseConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(HbaseConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 数据写入Phoenix
        PreparedStatement preparedStatement = null;
        try {
            JSONObject after = value.getJSONObject("after");
            Set<String> keySet = after.keySet();
            Collection<Object> values = after.values();

            JSONObject source = value.getJSONObject("source");
            // 表名
            String tableName = source.getString("table");
            // 创建插入数据的SQL
            String upsertSql = genUpsertSql(tableName, keySet, values);
            // 编译
            preparedStatement = connection.prepareStatement(upsertSql);
            // 执行
            preparedStatement.executeUpdate();
            // 提交
            connection.commit();
        } catch (SQLException e) {
            log.error("插入Phoenix数据失败",e.getMessage());
        } finally {
            if (preparedStatement == null) {
                preparedStatement.close();
            }
        }
    }

    private String genUpsertSql(String tableName, Set<String> keys, Collection<Object> values) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("upsert into ")
                .append(HbaseConfig.HBASE_SCHEMA).append(".").append(tableName)
                .append("(")
                .append(StringUtils.join(",", keys))
                .append(")")
                .append("values")
                .append("(")
                .append(StringUtils.join(",", values))
                .append(")");
        return buffer.toString();
    }
}
