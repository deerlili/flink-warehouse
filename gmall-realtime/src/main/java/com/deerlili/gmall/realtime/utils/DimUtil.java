package com.deerlili.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.common.HbaseConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * DimUtil hbase dim util
 *
 * @author lixx
 * @date 2022/7/27 9:40
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String columnName,String value) throws Exception {
        // 拼接查询语句
        String querySql = "select * from " + HbaseConfig.HBASE_SCHEMA + "." + tableName + " where " + columnName +"='"+ value +"'";
        System.out.println(querySql);
        // 查询Phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = queryList.get(0);
        // 返回结果
        return dimInfoJson;
    }

    public static void main(String[] args) throws Exception {
        Class.forName(HbaseConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(HbaseConfig.PHOENIX_SERVER);
        JSONObject dimInfo = getDimInfo(connection, "DIM_WARE_SKU", "ID", "29");
        System.out.println(dimInfo);
        connection.close();
    }
}
