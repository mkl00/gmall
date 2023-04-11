package com.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

/**
 * ES 工具类
 */
object MyESUtils {
  // 生命es客户端
  var esClient: RestHighLevelClient = bulid()

  /**
   * 批量数据幂等写入
   * 通过指定id实现幂等
   */
  def bulkSaveIdempotent(sourceList: List[(String, AnyRef)], indexName: String): Unit = {
    val bulkRequest: BulkRequest = new BulkRequest()

    for (source <- sourceList) {
      val indexRequest: IndexRequest = new IndexRequest()
      indexRequest.index(indexName)
      val sourceJson = JSON.toJSONString(source._2,new SerializeConfig(true))
      indexRequest.source(sourceJson,XContentType.JSON)
      indexRequest.id(source._1)
      bulkRequest.add(indexRequest)
    }
    esClient.bulk(bulkRequest,RequestOptions.DEFAULT)
  }

  /**
   * 批量写入设置
   */
  def bulkSave(sourceList: List[AnyRef], indexName: String) = {
    //    BulkSave 实际上就是由多个单条 IndexRequst 组成
    val bulkRequest: BulkRequest = new BulkRequest()
    for (source <- sourceList) {
      val indexRequest: IndexRequest = new IndexRequest()
      indexRequest.index(indexName)
      val sourceJson: String = JSON.toJSONString(source,new SerializeConfig(true))
      indexRequest.source(sourceJson,XContentType.JSON)
      bulkRequest.add(indexRequest)
    }
    esClient.bulk(bulkRequest,RequestOptions.DEFAULT)
  }

  /**
   * 单条数据 幂等写入
   * 通过指定id实现幂等写入
   */
  def saveIdempotent(source: (String, AnyRef), indexName: String) = {
    val indexRequest: IndexRequest = new IndexRequest()
    indexRequest.index(indexName)
    val str: String = JSON.toJSONString(source._2, new SerializeConfig(true))
    indexRequest.source(str, XContentType.JSON)
    indexRequest.id(source._1)
    esClient.index(indexRequest, RequestOptions.DEFAULT)
  }

  /**
   * 单条数据写入
   */
  def save(source: AnyRef, indexName: String) = {
    val request: IndexRequest = new IndexRequest()
    request.index(indexName)
    // 将bean类型转换成json类型
    val str: String = JSON.toJSONString(source, new SerializeConfig(true))
    request.source(str, XContentType.JSON)
    esClient.index(request, RequestOptions.DEFAULT)
  }

  /**
   * 销毁
   */
  def dostory(): Unit = {
    esClient.close()
    esClient == null
  }

  /**
   * 创建es客户端对象
   */
  def bulid(): RestHighLevelClient = {
    val builder: RestClientBuilder = RestClient.builder(new HttpHost("hadoop11", 9200))
    val client: RestHighLevelClient = new RestHighLevelClient(builder)
    client
  }

  /**
   * 获取es客户端
   */
  def getEsClient: RestHighLevelClient = {
    esClient
  }
}
