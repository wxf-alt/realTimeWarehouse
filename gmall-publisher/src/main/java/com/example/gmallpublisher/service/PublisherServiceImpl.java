package com.example.gmallpublisher.service;

import com.example.gmallpublisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auther: wxf
 * @Date: 2023/1/18 10:37:05
 * @Description: PublisherServiceImpl
 * @Version 1.0.0
 */
@Service  // 必须添加 Service 注解
public class PublisherServiceImpl implements PublisherService {

    /*自动注入 DauMapper 对象*/
    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        List<Map<String, Object>> hourDauList = dauMapper.getDauHour(date);
        HashMap<String, Long> hashMap = new HashMap<>();
        for (Map<String, Object> objectMap : hourDauList) {
            String hour = objectMap.get("hour").toString();
            Long count = (Long) objectMap.get("count");
            hashMap.put(hour, count);
        }
        return hashMap;
    }
}