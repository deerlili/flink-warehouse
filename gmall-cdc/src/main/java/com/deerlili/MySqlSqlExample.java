package com.deerlili;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author lixx
 * @date 2022/6/2
 * @notes flink cdc sql
 */
public class MySqlSqlExample {
    public static void main(String[] args) throws Exception {
        // create execute env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create flink-mysql-cdc source
        tableEnv.executeSql("CREATE TABLE base_trademark_binlog(" +
                "id STRING NOT NULL," +
                "tm_name STRING," +
                "logo_url STRING,PRIMARY KEY(id) NOT ENFORCED) " +
                "WITH (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='hadoop100'," +
                "'port'='3306'," +
                "'username'='test'," +
                "'password'='123456'," +
                "'database-name'='gmall_flink'," +
                "'table-name'='base_trademark')");

        // 第一种
        Table table = tableEnv.sqlQuery("select * from base_trademark_binlog");
        //DataStream<Tuple2<Boolean, Tuple2<Integer, Integer>>> dataStream = tableEnv.toRetractStream(table, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
        DataStream<Row> dataStream = tableEnv.toChangelogStream(table);
        dataStream.print();

        // 第二种
        //tableEnv.executeSql("select * from base_trademark_binlog").print();

        env.execute("Flink CDC With SQl");
    }
}
