package com.gmall.realtime.service;

import com.gmall.realtime.bean.ValueName;

import java.util.List;
import java.util.Map;

public interface VisitRealtimeService {
    Map<String, Object> queryDau(String td);

    List<ValueName> searchStatsByItem(String itemName, String date, String t);
}
