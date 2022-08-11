package com.deerlili.gmall.realtime.bean;

/**
 * ProductStats
 * <p>
 * Desc:  商品统计实体类
 * @Builder 注解可以使用构造者方式创建对象，给属性赋值
 * @Builder.Default 在使用构造者方式给属性赋值的时候，属性的初始值会丢失 该注解的作用就是修复这个问题
 * 例如：我们在属性上赋值了初始值为 0L，如果不加这个注解，通过构造者创建的对象属性值会变为 null
 * @author lixx
 */

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@Data
@Builder
public class ProductStats {

    /**
     * 窗口起始时间
     */
    String stt;
    /**
     * 窗口结束时间
     */
    String edt;
    /**
     * sku 编号
     */
    Long sku_id;
    /**
     * sku 名称
     */
    String sku_name;
    /**
     * sku 单价
     */
    BigDecimal sku_price;
    /**
     * spu 编号
     */
    Long spu_id;
    /**
     * spu名称
     */
    String spu_name;
    /**
     * 品牌编号
     */
    Long tm_id;
    /**
     * 品牌名称
     */
    String tm_name;
    /**
     * 品类编号
     */
    Long category3_id;
    /**
     * 品类名称
     */
    String category3_name;

    /**
     * 曝光数
     */
    @Builder.Default
    Long display_ct = 0L;

    /**
     * 点击数
     */
    @Builder.Default
    Long click_ct = 0L;

    /**
     * 收藏数
     */
    @Builder.Default
    Long favor_ct = 0L;

    /**
     * 添加购物车数
     */
    @Builder.Default
    Long cart_ct = 0L;

    /**
     * 下单商品个数
     */
    @Builder.Default
    Long order_sku_num = 0L;

    /**
     * 下单商品金额
     */
    @Builder.Default
    BigDecimal order_amount = BigDecimal.ZERO;

    /**
     * 订单数
     */
    @Builder.Default
    Long order_ct = 0L;

    /**
     * 支付金额
     */
    @Builder.Default
    BigDecimal payment_amount = BigDecimal.ZERO;

    /**
     * 支付订单数
     */
    @Builder.Default
    Long paid_order_ct = 0L;

    /**
     * 退款订单数
     */
    @Builder.Default
    Long refund_order_ct = 0L;

    /**
     * 退款金額
     */
    @Builder.Default
    BigDecimal refund_amount = BigDecimal.ZERO;

    /**
     * 评论订单数
     */
    @Builder.Default
    Long comment_ct = 0L;

    /**
     * 好评订单数
     */
    @Builder.Default
    Long good_comment_ct = 0L;

    /**
     * 用于统计订单数
     */
    @Builder.Default
    @TransientSink
    Set orderIdSet = new HashSet();
    /**
     * 用于统计支付订单数
     */
    @Builder.Default
    @TransientSink
    Set paidOrderIdSet = new HashSet();

    /**
     * 用于退款支付订单数
     */
    @Builder.Default
    @TransientSink
    Set refundOrderIdSet = new HashSet();
    /**
     * 统计时间戳
     */
    Long ts;

    public static void main(String[] args) {
        ProductStats productStats = ProductStats.builder()
                .sku_id(1L)
                .good_comment_ct(1L)
                .build();
    }
}

