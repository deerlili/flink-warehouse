package com.deerlili.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * PaymentInfo 支付实体类
 *
 * @author lixx
 * @date 2022/8/2 17:36
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;

}
