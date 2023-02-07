package com.example.gmallpublisher.service;

import com.example.gmallpublisher.mapper.DauMapper;
import com.example.gmallpublisher.mapper.OrderInfoMapper;
import common.Constant;
import common.ESUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
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
    @Autowired
    OrderInfoMapper orderInfoMapper;

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

    @Override
    public Double getTotalAmount(String date) {
        Double amount = orderInfoMapper.getTotalAmount(date);
        return null == amount ? 0 : amount;
    }

    @Override
    public Map<String, Double> getHourAmount(String date) {
        List<Map<String, Object>> hourAmount = orderInfoMapper.getHourAmount(date);
        HashMap<String, Double> hashMap = new HashMap<>();
        for (Map<String, Object> stringObjectMap : hourAmount) {
            String hour = stringObjectMap.get("CREATE_HOUR").toString();
            Double count = (Double) stringObjectMap.get("total_amount");
            hashMap.put(hour, count);
        }
        return hashMap;
    }

    // 根据参数 从 ES 获取数据
    @Override
    public Map<String, Object> getSaleDetailAndAggGroupByField(String date,
                                                               String keyWord,
                                                               int startPage,
                                                               int sizePerPage,
                                                               String aggField,
                                                               int aggCount) throws IOException {

        HashMap<String, Object> result = new HashMap<>();

        // 1.获取 ES 客户端
        JestClient jestClient = ESUtil.getClient();
        // 2. 查询数据
        String detalDSL = DSLs.getSaleDetailDSL(date, keyWord, startPage, sizePerPage, aggField, aggCount);
        Search search = new Search.Builder(detalDSL)
                .addIndex(Constant.INDEX_SALE_DETAIL)
                .addType("_doc")
                .build();
        SearchResult searchResult = jestClient.execute(search);
        // 3. 解析数据
        // 3.1 总数
        Integer total = searchResult.getTotal();
        result.put("total", total);
        // 3.2 明细
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        ArrayList<HashMap> detail = new ArrayList<>();
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            detail.add(source);
        }
        result.put("detail", detail);
        // 3.3 聚合结果
        HashMap<String, Long> aggMap = new HashMap<>();
        List<TermsAggregation.Entry> buckets = searchResult.getAggregations()
                .getTermsAggregation("group_" + aggField)
                .getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            String key = bucket.getKey();
            Long count = bucket.getCount();
            aggMap.put(key,count);
        }
        result.put("agg", aggMap);

        // 4. 返回最终结果
        return result;
    }


}