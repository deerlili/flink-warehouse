package com.deerlili.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * OrderDetail 订单明细
 *
 * @author lixx
 * @date 2022/7/18 10:37
 */
@Data
public class OrderDetail {
    Long id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;

}
