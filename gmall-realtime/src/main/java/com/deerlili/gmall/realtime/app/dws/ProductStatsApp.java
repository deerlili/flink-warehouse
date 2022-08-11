package com.deerlili.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.app.function.DimAsyncFunction;
import com.deerlili.gmall.realtime.app.function.DimAsyncMethodFunction;
import com.deerlili.gmall.realtime.bean.OrderWide;
import com.deerlili.gmall.realtime.bean.PaymentWide;
import com.deerlili.gmall.realtime.bean.ProductStats;
import com.deerlili.gmall.realtime.common.CfgConstant;
import com.deerlili.gmall.realtime.utils.ClickHouseUtil;
import com.deerlili.gmall.realtime.utils.DateFormatUtil;
import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * ProductStatsApp
 *
 * @author lixx
 * @date 2022/8/10 15:24
 * 数据流：
 *      app/web -> nginx -> springboot -> kafka(ods) -> flinkApp -> kafka(dwd) -> flinkApp -> ClickHouse
 *      app/web -> nginx -> springboot -> mysql -> flinkApp -> kafka(ods) -> flinkApp -> kafka(dwd)/phoenix -> flinkApp -> kafka(dwm) -> flinkApp -> clickHouse
 * 程 序：
 *     mock_db/mock_log -> nginx -> logger.sh -> kafka(zk)/Phoenix(Hdfs/hbase/zk) -> Redis -> clickHouse
 *     FlinkApp
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //构建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置检查点ck
        //env.setStateBackend(new FsStateBackend("HDFS://hadoop100:9000/flink/ck/visitor_stats_app"));
        //System.setProperty("HADOOP_USER_NAME","root");
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        //读取kafka7个主题创建数据流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwd_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic  = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pvDs = env.addSource(KafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorDs = env.addSource(KafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartDs = env.addSource(KafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderDs = env.addSource(KafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> payDs = env.addSource(KafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundDs = env.addSource(KafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentDs = env.addSource(KafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));

        //将数据统一格式
        /**
         *{
         * 	"common": {
         * 		"ar": "440000",
         * 		"ba": "iPhone",
         * 		"ch": "Appstore",
         * 		"is_new": "0",
         * 		"md": "iPhone 8",
         * 		"mid": "mid_167772",
         * 		"os": "iOS 13.2.3",
         * 		"uid": "3281",
         * 		"vc": "v2.1.134"
         *        },
         * 	"page": {
         * 		"during_time": 3401,
         * 		"item": "3,7",
         * 		"item_type": "sku_ids",
         * 		"last_page_id": "orders_unpaid",
         * 		"page_id": "trade"
         *    },
         * 	"ts": 1658742723000
         * }
         *
         * {"common":{"ar":"440000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone 8","mid":"mid_167772","os":"iOS 13.2.3","uid":"3281","vc":"v2.1.134"},
         *  "displays":[{"display_type":"activity","item":"1","item_type":"activity_id","order":1,"pos_id":5},{"display_type":"query","item":"3","item_type":"sku_id","order":2,"pos_id":3},{"display_type":"query","item":"6","item_type":"sku_id","order":3,"pos_id":5},{"display_type":"recommend","item":"2","item_type":"sku_id","order":4,"pos_id":2},{"display_type":"query","item":"1","item_type":"sku_id","order":5,"pos_id":2},{"display_type":"query","item":"8","item_type":"sku_id","order":6,"pos_id":3},{"display_type":"query","item":"2","item_type":"sku_id","order":7,"pos_id":2},{"display_type":"query","item":"9","item_type":"sku_id","order":8,"pos_id":4},{"display_type":"promotion","item":"3","item_type":"sku_id","order":9,"pos_id":4},{"display_type":"query","item":"2","item_type":"sku_id","order":10,"pos_id":4},{"display_type":"query","item":"10","item_type":"sku_id","order":11,"pos_id":3}],"page":{"during_time":17999,"page_id":"home"},"ts":1658742723000}
         * */
        SingleOutputStreamOperator<ProductStats> productClickAndDisplayDs = pvDs.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String s, Collector<ProductStats> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                Long ts = jsonObject.getLong("ts");

                //提取page信息
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");
                //点击数据
                if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                    collector.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        //取出单条曝光数据(有活动、商品、广告等)
                        JSONObject display = displays.getJSONObject(i);
                        String itemType = display.getString("item_type");
                        if ("sku_id".equals(itemType)) {
                            collector.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        //收藏表favor_info
        SingleOutputStreamOperator<ProductStats> productFavorDs = favorDs.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            return ProductStats.builder()
                    .spu_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //购物车表cart_info
        SingleOutputStreamOperator<ProductStats> productCartDs = cartDs.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            return ProductStats.builder()
                    .spu_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productOrderDs = orderDs.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            HashSet<Long> orderIds = new HashSet<>();//订单去重
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .spu_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getOrder_price())
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productPaymentDs = payDs.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());
            return ProductStats.builder()
                    .spu_id(paymentWide.getSpu_id())
                    .payment_amount(paymentWide.getOrder_price())
                    .paidOrderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        //退款-order_refund_info
        SingleOutputStreamOperator<ProductStats> productRefundDs = refundDs.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));
            return ProductStats.builder()
                    .spu_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productCommentDs = commentDs.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            String appraise = jsonObject.getString("appraise");
            Long goodCt = 0L;
            if (CfgConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }
            return ProductStats.builder()
                    .spu_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //Union 7个流
        DataStream<ProductStats> unionDs = productClickAndDisplayDs.union(
                productFavorDs,
                productCartDs,
                productOrderDs,
                productPaymentDs,
                productRefundDs,
                productCommentDs);

        //提取时间戳生成watermark
        SingleOutputStreamOperator<ProductStats> productWaterMarkDs = unionDs.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats productStats, long l) {
                        return productStats.getTs();
                    }
                }));
        //分组、开窗、集合 按照sku_id分组，10秒的滚动窗口，结合增量集合（累加值）和全量集合（提取窗口信息）
        SingleOutputStreamOperator<ProductStats> reduceDs = productWaterMarkDs.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
                        //取出数据
                        ProductStats productStats = iterable.iterator().next();
                        //设置窗口时间
                        productStats.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        productStats.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        //设置订单数量
                        productStats.setOrder_ct(productStats.getOrderIdSet().size() + 0L);
                        productStats.setPaid_order_ct(productStats.getPaidOrderIdSet().size() + 0L);
                        productStats.setRefund_order_ct(productStats.getRefundOrderIdSet().size() + 0L);
                        //将数据写入
                        collector.collect(productStats);

                    }
                });
        //7.关联维度信息
        //7.1.关联SKU
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDstream = AsyncDataStream.unorderedWait(reduceDs, new DimAsyncMethodFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats input) {
                return input.getSku_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) {
                productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                productStats.setTm_id(dimInfo.getLong("TM_ID"));
            }
        }, 60L, TimeUnit.SECONDS);
        //关联SPU
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream = AsyncDataStream.unorderedWait(productStatsWithSkuDstream,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getSpu_id());
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) {
                        productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                }, 60, TimeUnit.SECONDS);
        //关联TM 品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream = AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) {
                        productStats.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
        //关联Category 品类维度
        SingleOutputStreamOperator<ProductStats> productStatsDS = AsyncDataStream.unorderedWait(productStatsWithTmDstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) {
                        productStats.setCategory3_name(jsonObject.getString("NAME"));
                    }
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);



        //数据写入ClickHouse
        productStatsDS.addSink(ClickHouseUtil.<ProductStats>getClickHouseSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //启动任务
        env.execute("ProductStatsApp");
    }
}
