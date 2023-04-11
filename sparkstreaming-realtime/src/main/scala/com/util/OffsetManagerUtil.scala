package com.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * offset管理工具类，用于上redis中存储和读取offset
 *
 * 管理方案：
 *    1.后置提交偏移量 -》 手动控制偏移量提交
 *    2.手动控制偏移量提交-》sparkstreaming提供了手动提交的方案，但是不能用，因为我们会对DStream的结构进行转换，
 *    2.手动提交偏移量，维护到redis中
 * -》从kafka中获取数据，先提取偏移量
 * -》等到数据成功写出后，将偏移量存储到redis中
 * -》从kafka中消费数据之前，先到redis中读取偏移量，使用读取到的偏移量，到kafka中消费数据
 * 从redis中读取偏移量，
 * Redis 格 式 ： type=>Hash [key=>offset:topic:groupId
 * field=>partitionId value=>偏移量值] expire 不需要指定
 */
object OffsetManagerUtil {

  /**
   * 往redis中存储数据  offset
   * 问题：存的offset从哪里啊？
   * 从消费到的数据中提取出来，传入到该方法中，
   * offsetRange: Array[OffsetRange]
   * offset的结构是什么？
   * kafka中维护的offset的结构
   * groupid + topic + partition =》 offset
   * 从传入进来的offset中提取关键信息
   * redis中怎么存
   *
   */
  def savnOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]) = {
    if (offsetRanges != null && offsetRanges.length > 0) {
      val offsets: util.HashMap[String, String] = new util.HashMap[String, String]()
      for (offsetRange <- offsetRanges) {
        val partition: Int = offsetRange.partition
        val endOffset = offsetRange.untilOffset
        offsets.put(partition.toString, endOffset.toString)
      }
      // 往redis里存
      val jedis: Jedis = MyRedisUtils.getRedisPool()
      val redisKey = s"offset:$topic:$groupId"
      jedis.hset(redisKey, offsets)

      jedis.close()
    }
  }

  /**
   * 从redis中读取数据
   * 1.问题：如何通过sparkstreaming从指定的offset进行消费
   *
   * sparkstreaming中的offset格式为：Map[TopicPatition,long]
   */
  def readOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    // 获取redis客户端
    val jedis = MyRedisUtils.getRedisPool()
    // 拼接redis中存储的偏移量的key
    val redisKey = s"offset:$topic:$groupId"
    //根据 key 从 Reids 中获取数据
    val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)
    // 关闭redis
    jedis.close()
    var results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    // 将java的map转换成scala的map进行迭代
    import scala.collection.JavaConverters._
    for ((partition, endOffset) <- offsets.asScala) {
      val topicPatition: TopicPartition = new TopicPartition(topic, partition.toInt)
      results.put(topicPatition, endOffset.toLong)
    }
    results.toMap
  }

}