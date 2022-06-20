package com.deerlil.gmall.realtime.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

/**
 * @author lixx
 * @date 2022/6/11
 * @notes mysql table process configure
 */
public class MySqlUtil {

    public static MySqlSource<String> getMysqlConfigSource() {
        MySqlSource<String> mysqlSource = new MySqlSourceBuilder<String>()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("realtime")
                .tableList("realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        return mysqlSource;
    }

    public static MySqlSource<String> getMysqlBaseSource() {
        MySqlSource<String> mysqlSource = new MySqlSourceBuilder<String>()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("gmall_flink")
                .tableList("gmall_flink.*")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        return mysqlSource;
    }
}
