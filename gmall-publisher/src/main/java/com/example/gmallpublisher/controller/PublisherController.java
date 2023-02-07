package com.example.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.example.gmallpublisher.bean.Option;
import com.example.gmallpublisher.bean.SaleInfo;
import com.example.gmallpublisher.service.PublisherServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.example.gmallpublisher.bean.Stat;

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
    // http://localhost:8070/realtime-total?date=2023-01-29
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

        HashMap<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", publisherService.getTotalAmount(date).toString());
        list.add(map3);

        return JSON.toJSONString(list);
    }

    // http://localhost:8070/realtime-hour?id=dau&date=2020-02-11
    // http://localhost:8070/realtime-hour?id=order_amount&date=2023-01-29
    @GetMapping("/realtime-hour")
    public String realtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        if ("dau".equals(id)) {
            Map<String, Long> toDayCount = publisherService.getDauHour(date);
            Map<String, Long> yesterDayCount = publisherService.getDauHour(getYesterday(date));
            Map<String, Map<String, Long>> resultMap = new HashMap<>();
            resultMap.put("today", toDayCount);
            resultMap.put("yesterday", yesterDayCount);
            return JSON.toJSONString(resultMap);
        } else if ("order_amount".equals(id)) {
            Map<String, Double> toDayCount = publisherService.getHourAmount(date);
            Map<String, Double> yesterDayCount = publisherService.getHourAmount(getYesterday(date));
            Map<String, Map<String, Double>> resultMap = new HashMap<>();
            resultMap.put("today", toDayCount);
            resultMap.put("yesterday", yesterDayCount);
            return JSON.toJSONString(resultMap);
        } else {
            return "";
        }
    }

    //  http://localhost:8070/sale_detail?date=2019-05-20&&startpage=1&&size=5&&keyword=手机小米
    @GetMapping("/sale_detail")
    public String saleDetail(@RequestParam("date") String date,
                             @RequestParam("startpage") int startpage,
                             @RequestParam("size") int size,
                             @RequestParam("keyword") String keyword) throws IOException {

        Map<String, Object> resultGender = publisherService.getSaleDetailAndAggGroupByField(
                date,
                keyword,
                startpage, size,
                "user_gender",
                2);
        Map<String, Object> resultAge = publisherService.getSaleDetailAndAggGroupByField(
                date,
                keyword,
                startpage, size,
                "user_age",
                100);

        // 最终返回结果
        SaleInfo saleInfo = new SaleInfo();
        // 1.封装总数（两个结果中任何一个都可以）
        saleInfo.setTotal((Integer) resultGender.get("total"));
        // 2.封装明细（两个结果集都是一样的(ES)）
        List<Map<String, Object>> detail = (List<Map<String, Object>>) resultGender.get("detail");
        saleInfo.setDetail(detail);
        // 3.封装 聚合结果 饼图
        // 3.1 性别饼图
        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别占比");
        Map<String, Long> genderAgg = (Map<String, Long>) resultGender.get("agg");
        for (String key : genderAgg.keySet()) {
            Option opt = new Option();
            String gender = ("M".equals(key) ? "男" : "女");
            opt.setName(gender);  // 饼图性别
            opt.setValue(genderAgg.get(key));  // 性别对应个数
            genderStat.addOptions(opt); // 添加到性别饼图中
        }
        saleInfo.addStat(genderStat);
        // 3.2 年龄饼图
        Stat ageStat = new Stat();
        ageStat.addOptions(new Option("20岁以下", 0L));
        ageStat.addOptions(new Option("20岁到30岁", 0L));
        ageStat.addOptions(new Option("30岁及以上", 0L));
        ageStat.setTitle("用户年龄占比");
        Map<String, Long> ageAgg = (Map<String, Long>) resultAge.get("agg");
        for (String key : ageAgg.keySet()) {
            int age = Integer.parseInt(key);
            Long value = ageAgg.get(key);
            if ((age < 20)) {
                Option opt = ageStat.getOptions().get(0);
                opt.setValue(opt.getValue() + value);
            } else if (age < 30) {
                Option opt = ageStat.getOptions().get(1);
                opt.setValue(opt.getValue() + value);
            } else {
                Option opt = ageStat.getOptions().get(2);
                opt.setValue(opt.getValue() + value);
            }
        }
        saleInfo.addStat(ageStat);

        return JSON.toJSONString(saleInfo);
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

3.总销售额
[{"id":"dau","name":"新增日活","value":1200},
{"id":"new_mid","name":"新增设备","value":233 },
{"id":"order_amount","name":"新增交易额","value":1000.2 }]

4.小时销售额
{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
"today":{"12":38,"13":1233,"17":123,"19":688 }}

 */