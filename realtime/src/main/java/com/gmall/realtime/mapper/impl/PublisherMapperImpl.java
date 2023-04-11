package com.gmall.realtime.mapper.impl;

import com.gmall.realtime.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.annotations.IndexPrefixes;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

    @Autowired
    RestHighLevelClient esClient;

    final String indexNamePrefix = "gmall_dau_info_";

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
        String indexName = indexNamePrefix + td;
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

        String indexName = indexNamePrefix + td;
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
