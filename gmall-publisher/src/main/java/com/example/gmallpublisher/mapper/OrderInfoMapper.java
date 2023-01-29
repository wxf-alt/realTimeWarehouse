package com.example.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderInfoMapper {

    // 总销售额
    Double getTotalAmount(String date);

    // 小时销售额
    // Map(hour->"10",sum -> 200.1)
    // Map(hour->"11",sum -> 135.4)
    List<Map<String, Object>> getHourAmount(String date);

}
