package com.deerlili.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;

/**
 * DimAsyncMethodFunction 提取DimAsyncFunction抽象类
 *
 * @author lixx
 * @date 2022/8/1 16:38
 */
public interface DimAsyncMethod<T> {

    /**
     * 获取表的id
     * @param input
     * @return
     */
    String getKey(T input);

    /**
     * kafkaStream和查询维度信息关联
     * @param input
     * @param dimInfo
     */
    void join(T input, JSONObject dimInfo);
}
