package com.example.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.example.gmallpublisher.service.PublisherServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: wxf
 * @Date: 2023/1/18 10:45:46
 * @Description: PublisherController
 * @Version 1.0.0
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherServiceImpl publisherService;

    // http://localhost:8070/realtime-total?date=2020-02-11
    @GetMapping("/realtime-total")
    public String realtimeTotal(@RequestParam("date") String date) {
        ArrayList<Map<String, String>> list = new ArrayList<>();

        HashMap<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", publisherService.getDauTotal(date).toString());
        list.add(map1);

        HashMap<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        list.add(map2);

        return JSON.toJSONString(list);
    }

    // http://localhost:8070/realtime-hour?id=dau&date=2020-02-11
    @GetMapping("/realtime-hour")
    public String realtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        if ("dau".equals(id)) {
            Map<String, Long> toDayCount = publisherService.getDauHour(date);
            Map<String, Long> yesterDayCount = publisherService.getDauHour(getYesterday(date));
            Map<String, Map<String, Long>> resultMap = new HashMap<>();
            resultMap.put("today", toDayCount);
            resultMap.put("yesterday", yesterDayCount);
            return JSON.toJSONString(resultMap);
        } else {
            return "";
        }
    }

    private String getYesterday(String date) {
//        LocalDate result = LocalDate.parse(date).plusDays(-1);
        LocalDate result = LocalDate.parse(date).minusDays(1);
        return result.toString();
    }

}

/*
1.日活
[{"id":"dau","name":"新增日活","value":1200},
{"id":"new_mid","name":"新增设备","value":233} ]

2.日活分时统计
{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
"today":{"12":38,"13":1233,"17":123,"19":688 }}
 */