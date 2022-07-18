package com.deerlili.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * OrderInfo 订单信息
 *
 * @author lixx
 * @date 2022/7/18 10:29
 */
@Data
public class OrderInfo {
    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    /*
    * 由create_time把其他字段处理得到
    */
    String create_date;
    /*
     * 由create_time把其他字段处理得到
     */
    String create_hour;
    /*
     * 由create_time把其他字段处理得到
     */
    Long create_ts;

}
