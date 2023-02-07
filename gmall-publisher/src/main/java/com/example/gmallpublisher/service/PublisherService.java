package com.example.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {

    //     查询 日活
    Long getDauTotal(String date);

    // 查询小时明细
    // 相比数据层, 我们把数据结构做下调整, 更方便使用
    Map<String, Long> getDauHour(String date);

    // 总销售额
    Double getTotalAmount(String date);

    // 小时销售额
    Map<String, Double> getHourAmount(String date);


    /**
     * @param date
     * @param keyWord
     * @param startPage
     * @param sizePerPage
     * @param aggField
     * @param aggCount
     * @Description:
     * @Author: wxf
     * @Date: 2023/2/7 15:31
     * @return: void
     * {
     * "total": 100,
     * "stat" : [ {  //年龄段比例  }, {  //男女比例 } ],
     * "detail": { //明细 }
     * }
     **/
    Map<String, Object> getSaleDetailAndAggGroupByField(String date,
                                                        String keyWord,
                                                        int startPage,
                                                        int sizePerPage,
                                                        String aggField,
                                                        int aggCount) throws IOException;

}
