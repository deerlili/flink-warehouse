package com.deerlili.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author deerlili
 * @date 2022/5/31
 * @des test
 */
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @GetMapping("/test")
    public String test() {
        return "success";
    }

    @GetMapping("/applog")
    public String applog(@RequestParam(name = "param") String param) {
        // 数据落盘
        log.info(param);
        // 数据写入 Kafka
        kafkaTemplate.send("ods_base_log",param);
        return "success";
    }

}
