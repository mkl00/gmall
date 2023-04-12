package com.gmall.realtime.mapper;

import com.gmall.realtime.bean.ValueName;

import java.util.List;
import java.util.Map;

public interface PublisherMapper {
    Map<String, Object> searchDau(String td);

    List<ValueName> searchStatsByItem(String itemName, String date, String field);

    Map<String, Object> detailByItem(String date, String itemName, Integer from, Integer pageSize);
}
