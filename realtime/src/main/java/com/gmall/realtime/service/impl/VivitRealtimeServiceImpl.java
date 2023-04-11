package com.gmall.realtime.service.impl;

import com.gmall.realtime.bean.ValueName;
import com.gmall.realtime.mapper.PublisherMapper;
import com.gmall.realtime.service.VisitRealtimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 业务层
 */
@Service
public class VivitRealtimeServiceImpl implements VisitRealtimeService {

    @Autowired
    PublisherMapper publisherMapper;

    @Override
    public List<ValueName> searchStatsByItem(String itemName, String date, String t) {
        publisherMapper.
        return null;
    }

    /**
     * 日活分析
     * @param td
     * @return
     */
    @Override
    public Map<String, Object> queryDau(String td) {
        Map<String, Object> results=publisherMapper.searchDau(td);
        return results;
    }

}
