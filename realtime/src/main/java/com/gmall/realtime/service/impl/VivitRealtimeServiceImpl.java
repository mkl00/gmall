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

    /**
     * 计算明细列表
     *
     * @param date
     * @param itemName
     * @param pageNo   需要计算出来数据的位置
     * @param pageSize
     * @return
     */
    @Override
    public Map<String, Object> detailByItem(String date, String itemName, Integer pageNo, Integer pageSize) {
        Integer from = (pageNo - 1) * pageSize;
        Map<String, Object> result = publisherMapper.detailByItem(date, itemName, from, pageSize);
        return result;
    }

    @Override
    public List<ValueName> searchStatsByItem(String itemName, String date, String t) {
        String field = null;
        if ("age".equals(t)) {
            field = "user_age";
        } else if ("gender".equals(t)) {
            field = "user_gender";
        }
        List<ValueName> valueNames = publisherMapper.searchStatsByItem(itemName, date, field);

        if ("user_gender".equals(field)) {
            for (ValueName valueName : valueNames) {
                String gender = valueName.getName();
                if ("F".equals(gender)) {
                    valueName.setName("女");
                } else {
                    valueName.setName("男");
                }
            }
        } else if ("user_age".equals(field)) {
            double totalAmount20 = 0d;
            double totalAmount20to30 = 0d;
            double totalAmount30 = 0d;

            for (ValueName valueName : valueNames) {
                int age = Integer.parseInt(valueName.getName());
                if (age < 20) {
                    totalAmount20 += valueName.getValue();
                } else if (age < 30) {
                    totalAmount20to30 += valueName.getValue();
                } else {
                    totalAmount30 += valueName.getValue();
                }
            }

            valueNames.clear();
            valueNames.add(new ValueName(totalAmount20, "20岁以下"));
            valueNames.add(new ValueName(totalAmount20to30, "20岁至29岁"));
            valueNames.add(new ValueName(totalAmount30, "30岁以上"));

        }
        return valueNames;
    }


    /**
     * 日活分析
     *
     * @param td
     * @return
     */
    @Override
    public Map<String, Object> queryDau(String td) {
        Map<String, Object> results = publisherMapper.searchDau(td);
        return results;
    }

}
