package com.deerlili.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.common.HbaseConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * DimUtil hbase dim util
 *
 * @author lixx
 * @date 2022/7/27 9:40
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
        // 查询Phoenix之前先查Redis
        Jedis jedis = RedisUtil.getJedis();
        // DIM:DIM_USER_INFO:143
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfo = jedis.get(redisKey);
        if (dimInfo != null) {
            // 重置过期时间
            jedis.expire(redisKey, 24*60*60);
            // 归还连接
            jedis.close();
            // 返回结果
            return JSON.parseObject(dimInfo);
        }

        // 拼接查询语句
        String querySql = "select * from " + HbaseConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id +"'";
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
        Jedis jedis = RedisUtil.getJedis();
        jedis.del("DIM:" + tableName + ":" + id);
        jedis.close();
    }
}
