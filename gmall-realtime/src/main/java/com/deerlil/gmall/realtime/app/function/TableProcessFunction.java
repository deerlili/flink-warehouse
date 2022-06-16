package com.deerlil.gmall.realtime.app.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.deerlil.gmall.realtime.bean.TableProcess;
import com.deerlil.gmall.realtime.common.HbaseConfig;

import lombok.extern.slf4j.Slf4j;

/**
 * TableProcessFunction mysql table process config function
 *
 * @author lixx
 * @date 2022/6/11 23:57
 **/

@Slf4j
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> hbaseTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> hbaseTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.hbaseTag = hbaseTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(HbaseConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(HbaseConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String s,
        BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector)
        throws Exception {
        /*
        * 1.获取并解析数据
        * 2.建表
        * 3.写入状态，广播出去
        * */
        JSONObject data = JSON.parseObject(s);
        String after = data.getString("after");
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);

        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());
        }

        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSinkTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
        
    }

    // 建表方法
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk.isEmpty()) {
                sinkPk = "id";
            }

            if (sinkExtend.isEmpty()) {
                sinkExtend = "";
            }

            StringBuilder tableTableSQL = new StringBuilder("create table if not exists ")
                .append(HbaseConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                // 判断是否为主键
                if (sinkPk.equals(column)) {
                    tableTableSQL.append(column).append(" ").append("varchar primary key");
                } else {
                    tableTableSQL.append(column).append(" ").append("varchar");
                }
                // 判断时候为最后一个字段,不是添加逗号
                if (i < columns.length - 1) {
                    tableTableSQL.append(",");
                }
            }
            tableTableSQL.append(")").append(sinkExtend);
            log.info(tableTableSQL.toString());

            // 预编译SQL
            preparedStatement = connection.prepareStatement(tableTableSQL.toString());

            // 执行
            preparedStatement.execute();
        } catch (SQLException e) {
            log.error(String.valueOf(e));
            throw new RuntimeException("Phoenix表"+sinkTable+"建表异常！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void processElement(JSONObject jsonObject,
        BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext,
        Collector<JSONObject> collector) throws Exception {
        /*
        * 1.获取状态数据
        * 2.过滤字段
        * 3.分流
        * */

        // 1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        JSONObject source = jsonObject.getJSONObject("source");
        String key = source.getString("db") + "-" + source.getString("table");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            // 2.过滤字段
            JSONObject after = jsonObject.getJSONObject("after");
            filterColumn(after,tableProcess.getSinkColumns());

            // 3.分流
            // 将输出表和主题信息写入jsonObject
            jsonObject.put("sinkTable",tableProcess.getSinkTable());

            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                // Kafka数据，将数据输出到主流
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                // Hbase数据，将数据输出到侧输出流
                readOnlyContext.output(hbaseTag,jsonObject);
            }
        } else {
            log.warn("该组合不存在，Key为{}：", key);
        }
    }

    /**
     * 过滤方法
     * @param after         {"id":2,"tm_name":"苹果","logo_url":"/static/default.jpg"}
     * @param sinkColumns   id,tm_name
     */
    private void filterColumn(JSONObject after, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

        // Iterator<Map.Entry<String, Object>> iterator = after.entrySet().iterator();
        // while (iterator.hasNext()) {
        //     Map.Entry<String, Object> next = iterator.next();
        //     if (!columns.contains(next.getKey())) {
        //         iterator.remove();
        //     }
        // }

        after.entrySet().removeIf(next -> !columns.contains(next.getKey()));

    }
}
