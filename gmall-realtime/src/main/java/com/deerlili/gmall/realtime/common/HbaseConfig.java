package com.deerlili.gmall.realtime.common;

/**
 * HbaseConfig hbase jdbc config info
 *
 * @author lixx
 * @date 2022/6/12 11:14
 */

public class HbaseConfig {
    /**
     * Phoenix 库名
     */
    public static final String HBASE_SCHEMA = "REALTIME";
    /**
     * Phoenix 驱动
     */
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    /**
     * Phoenix 参数
     */
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    /**
     * clickHouse url
     */
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop100:8123/default";
    /**
     * clickHouse 驱动
     */
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";


}
