package com.deerlil.gmall.realtime.app.function;

import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;

/**
 * TableProcessFunction mysql table process config function
 *
 * @author lixx
 * @date 2022/6/11 23:57
 **/

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        /*
        * 1.获取并解析数据
        * 2.建表
        * 3.写入状态，广播出去
        * */
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        /*
        * 1.获取状态数据
        * 2.过滤字段
        * 3.分流
        * */
    }
}
