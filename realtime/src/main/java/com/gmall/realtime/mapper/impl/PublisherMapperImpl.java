package com.gmall.realtime.mapper.impl;

import com.gmall.realtime.bean.ValueName;
import com.gmall.realtime.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

    @Autowired
    RestHighLevelClient esClient;

    private String dauIndexNamePrefix = "gmall_dau_info_";
    private String orderIndexNamePrefix = "gmall_order_wide_";

    /**
     * 明细列表
     *
     * @param date
     * @param itemName
     * @param from
     * @param pageSize
     * @return
     */
    @Override
    public Map<String, Object> detailByItem(String date, String itemName, Integer from, Integer pageSize) {
        Map<String, Object> result = new HashMap<>();
        String indexName = orderIndexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 明细字段
        sourceBuilder.fetchSource(
                new String[]{"create_time","order_price","province_name","sku_name","sku_num","total_amount","user_age"},null);

        // query
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        sourceBuilder.query(matchQueryBuilder);
        //from
        sourceBuilder.from(from);
        // size
        sourceBuilder.size(pageSize);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse search = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            SearchHit[] hits = search.getHits().getHits();
            List<Map<String,Object>> detail = new ArrayList<>();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                detail.add(sourceAsMap);
            }
            result.put("total",total);
            result.put("detail",detail);

        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在");
            }
        } catch (IOException e) {
            throw new RuntimeException("查询ES失败。。。。。");
        }
        return result;
    }


    /**
     * 业务分析
     * age=》user_age  gender=> user_gender
     *
     * @return
     */
    @Override
    public List<ValueName> searchStatsByItem(String itemName, String date, String field) {
        List<ValueName> valueNameList = new ArrayList<>();
        String indexName = orderIndexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 去掉明细
        sourceBuilder.size(0);
        //query
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        sourceBuilder.query(queryBuilder);
        //group
        TermsAggregationBuilder termsAggregationBuilder =
                AggregationBuilders.terms("groupby" + field).field(field).size(100);
        //sum
        SumAggregationBuilder sumAggregationBuilder =
                AggregationBuilders.sum("totalamount").field("split_total_amount");
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        sourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupby" + field);
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String keyAsString = bucket.getKeyAsString();
                long docCount = bucket.getDocCount();
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedSum parsedSum = bucketAggregations.get("totalamount");
                double value = parsedSum.getValue();
                valueNameList.add(new ValueName(value, keyAsString));
            }
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在");
            }
        } catch (IOException e) {
            throw new RuntimeException("查询ES失败。。。。。");
        }

        return valueNameList;
    }

    /**
     * 日志处理
     *
     * @param td
     * @return
     */
    @Override
    public Map<String, Object> searchDau(String td) {
        Map<String, Object> results = new HashMap<>();
        Long total = searchDauTotal(td);
        results.put("dauTotal", total);

        // 今日分时明细
        Map<String, Long> dauTd = SearchDauHr(td);
        results.put("dauTd", dauTd);

        // 昨日分时明细
        LocalDate tdLd = LocalDate.parse(td);
        LocalDate ydLd = tdLd.minusDays(1);
        Map<String, Long> dauYd = SearchDauHr(ydLd.toString());
        results.put("dauYd", dauYd);

        return results;
    }


    public Map<String, Long> SearchDauHr(String td) {
        Map<String, Long> dauHr = new HashMap<>();
        // 组合索引名字
        String indexName = dauIndexNamePrefix + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 不要明细
        sourceBuilder.size(0);
        // 聚合
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupbyhr").field("hr").size(24);
        sourceBuilder.aggregation(aggregationBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse search = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = search.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupbyhr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String keyAsString = bucket.getKeyAsString();
                long docCount = bucket.getDocCount();
                dauHr.put(keyAsString, docCount);
            }

        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在");
            }
        } catch (IOException e) {
            throw new RuntimeException("查询ES失败。。。。。");
        }

        return dauHr;
    }

    public Long searchDauTotal(String td) {

        String indexName = dauIndexNamePrefix + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 不要明细
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse search = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long totalDau = search.getHits().getTotalHits().value;
            return totalDau;
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + "不存在");
            }
        } catch (IOException e) {
            throw new RuntimeException("查询ES失败。。。。。");
        }
        return 0L;
    }
}
