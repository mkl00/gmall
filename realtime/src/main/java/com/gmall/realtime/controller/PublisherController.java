package com.gmall.realtime.controller;

import com.gmall.realtime.bean.ValueName;
import com.gmall.realtime.service.VisitRealtimeService;
import org.elasticsearch.common.recycler.Recycler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;


/**
 * 控制层
 */
@RestController
public class PublisherController {

    @Autowired
    VisitRealtimeService visitRealtimeService;

    /**
     * 明细列表
     * http://bigdata.gmall.com/detailByItem?date=2021-02-02&itemName=小米手机&pageNo=1&pageSize=20
     */
    @GetMapping("detailByItem")
    public Map<String, Object> detailByItem(@RequestParam("date") String date,
                                            @RequestParam("itemName") String itemName,
                                            @RequestParam(value = "pageNo", required = false, defaultValue = "1") Integer pageNo,
                                            @RequestParam(value = "pageSize") Integer pageSize) {
        Map<String, Object> result = visitRealtimeService.detailByItem(date, itemName, pageNo, pageSize);
        return result;
    }

    /**
     * 交易分析
     * 统计图—年龄段
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=gender
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=age
     *
     * @return
     */
    @GetMapping("statsByItem")
    public List<ValueName> statsByItem(@RequestParam("itemName") String itemName,
                                       @RequestParam("date") String date,
                                       @RequestParam("t") String t) {
        List<ValueName> result = visitRealtimeService.searchStatsByItem(itemName, date, t);
        return result;
    }

    /**
     * 日活分析
     *
     * @param td
     * @return
     */
    @GetMapping("dauRealtime")
    public Map<String, Object> duaRealtime(String td) {
        Map<String, Object> results = visitRealtimeService.queryDau(td);
        return results;
    }

}
