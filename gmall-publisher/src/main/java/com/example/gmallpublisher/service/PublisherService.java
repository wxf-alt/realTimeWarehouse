package com.example.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {

    //     查询 日活
    Long getDauTotal(String date);

    // 查询小时明细
    // 相比数据层, 我们把数据结构做下调整, 更方便使用
    Map<String, Long> getDauHour(String date);

}
