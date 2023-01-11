package com.example.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * @Auther: wxf
 * @Date: 2023/1/11 09:46:27
 * @Description: LoggerController
 * @Version 1.0.0
 */
//@Controller
//@ResponseBody
@RestController // 等价于 @Controller + @ResponseBody
public class LoggerController {

    // 初始化 Logger 对象
    private final Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @PostMapping("/log")
    public String logger(@RequestParam("log") String log) {
        // 日志转成 JSONObject
        JSONObject logObj = JSON.parseObject(log);
        // 添加时间戳
        logObj = addTs(logObj);
        // 日志落盘
        saveLog(logObj);
//        // 发送到 kafka
        sendToKafka(logObj);
        return "success";
    }

    /**
     * @param logObj
     * @Description: 添加时间戳
     * @Author: wxf
     * @Date: 2023/1/11 10:08
     * @return: JSONObject
     **/
    public JSONObject addTs(JSONObject logObj) {
        logObj.put("ts", System.currentTimeMillis());
        return logObj;
    }

    /**
     * @param logObj
     * @Description: 日志落盘  使用 log4j
     * @Author: wxf
     * @Date: 2023/1/11 10:07
     * @return: void
     **/
    public void saveLog(JSONObject logObj) {
        logger.info(logObj.toJSONString());
    }


    // 使用注入的方式来实例化 KafkaTemplate. Spring boot 会自动完成
    @Autowired
    KafkaTemplate template;

    /**
     * @param logObj
     * @Description: 发送日志到 kafka
     * @Author: wxf
     * @Date: 2023/1/11 10:12
     * @return: void
     **/
    private void sendToKafka(JSONObject logObj) {
        String logType = logObj.getString("logType");
        String topicName = Constant.TOPIC_STARTUP;

        if ("event".equals(logType)) {
            topicName = Constant.TOPIC_EVENT;
        }
        template.send(topicName, logObj.toJSONString());
    }

//    @PostMapping("/log")
//    public String logger(@RequestParam("log") String log){
//        //System.out.println(log);
//
//        //加时间戳
//        log = addTs(log);
//        System.out.println(log);
//        //写磁盘
//        saveLog(log);
//
//        //写到kafka  先创建kafak生产者 再调生产者send方法
//        sendToKafka(log);
//        return "666";
//    }
//
//    @Autowired
//    KafkaTemplate template;
//    private void sendToKafka(String log) {
//        String topic = Constant.TOPIC_STARTUP ;
//        if(log.contains("event")){
//            topic= Constant.TOPIC_EVENT;
//        }
//        template.send(topic,log);
//    }
//
//    private Logger logger = LoggerFactory.getLogger(LoggerController.class);
//    private void saveLog(String log) {
//        logger.info(log);
//    }
//
//    private String addTs(String log) {
//        JSONObject obj = JSON.parseObject(log);
//        obj.put("ts",System.currentTimeMillis());
//        return obj.toJSONString();
//    }

}


/**
 * 业务:
 * <p>
 * 1. 给日志添加时间戳 (客户端的时间有可能不准, 所以使用服务器端的时间)
 * <p>
 * 2. 日志落盘
 * <p>
 * 3. 日志发送 kafka
 */






