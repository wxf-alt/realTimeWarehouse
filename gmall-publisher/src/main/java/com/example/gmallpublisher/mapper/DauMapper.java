package com.example.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

// 从数据库查询数据的接口
public interface DauMapper {

    // 查询日活总数
    Long getDauTotal(String date);

    // 查询小时日活
    List<Map<String,Object>> getDauHour(String date);


}
