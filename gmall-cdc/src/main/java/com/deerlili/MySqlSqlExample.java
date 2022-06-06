package com.deerlili;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
        tableEnv.executeSql("CREATE TABLE USER_INFO(" +
                "id int," +
                "name string," +
                "phone String) " +
                "WITH (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='hadoop100'," +
                "'port'='3306'," +
                "'username'='test'," +
                "'password'='123456'," +
                "'database-name'='gmall_flink'," +
                "'table-name'='z_user_info')");

        tableEnv.executeSql("select * from gmall_flink.user_info").print();

        env.execute();
    }
}
