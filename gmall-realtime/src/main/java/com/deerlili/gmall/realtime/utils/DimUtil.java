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

    public static JSONObject getDimInfo(Connection connection, String tableName,String id) throws Exception {
        //查询Phoenix之前先查Redis
        Jedis jedis = RedisUtil.getJedis();
        //DIM:DIM_USER_INFO:143
        String redisKey = "DIM:" + tableName  + ":" + id;
        String dimInfo = jedis.get(redisKey);
        if (dimInfo != null) {
            //重置过期时间
            jedis.expire(redisKey, 24*60*60);
            //归还连接
            jedis.close();
            //返回结果
            return JSON.parseObject(dimInfo);
        }

        // 拼接查询语句
        String querySql = "select * from " + HbaseConfig.HBASE_SCHEMA + "." + tableName + " where id = '"+ id +"'";
        System.out.println(querySql);
        // 查询Phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = queryList.get(0);

        // 在返回结果之前将数据写入Redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24*60*60);
        jedis.close();

        // 返回结果
        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName,String id) {
        //DIM:DIM_USER_INFO:ID:143
        String redisKey = "DIM:" + tableName  + ":" + id;

        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(HbaseConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(HbaseConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
        JSONObject dimInfo = getDimInfo(connection, "DIM_BASE_TRADEMARK", "30");
        System.out.println(dimInfo);
        long end = System.currentTimeMillis();
        JSONObject dimInfo1 = getDimInfo(connection, "DIM_BASE_TRADEMARK", "30");
        System.out.println(dimInfo1);
        long end1 = System.currentTimeMillis();
        System.out.println(end-start);
        System.out.println(end1-end);
        connection.close();
        /**
         * select * from REALTIME.DIM_WARE_SKU where ID='29'
         * {"SKU_ID":"29","ID":"29"}
         * select * from REALTIME.DIM_WARE_SKU where ID='29'
         * {"SKU_ID":"29","ID":"29"}
         * 307
         * 11
         * Hbase 有客户端缓存，第二次查比第一次查快，那么需要优化
         */

        // 添加redis后测试
        JSONObject dimInfo2 = getDimInfo(connection, "DIM_BASE_TRADEMARK", "30");
        System.out.println(dimInfo2);
        long end2 = System.currentTimeMillis();
        System.out.println(end2-end1);
        /**
         *{"SKU_ID":"29","ID":"29"}
         * 409
         * 33
         * 连接池：0
         * {"SKU_ID":"29","ID":"29"}
         * 1
         */
    }
}
