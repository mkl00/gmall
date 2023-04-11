package com.util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable


/**
 * kafka 工具类，用于 消费和生产kafka数据
 */
object MyKafkaUtils {

  // 配置consumer 信息
  private val consumerConfig: mutable.Map[String, Object] = mutable.Map[String, Object](
    //kafka 配置信息
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtils("kafka.broker.list"),
    //kv 反序列化
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // groupid
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall",
    // offset 提交：自动、手动
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    // offset 重置
    //...
  )

  /**
   * 基于sparkstreaming消费，获取fakfaDStream 使用默认的offset
   */

  // 自动获取偏移量
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String) = {
    consumerConfig(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfig)
    )

    kafkaDStream
  }

  // 手动获取偏移量
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offset: Map[TopicPartition, Long]) = {
    consumerConfig(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfig, offset)
    )

    kafkaDStream
  }

  /**
   * 创建kafka生产者
   */
  def kafkaProducer(): KafkaProducer[String, String] = {
    var prop = new Properties()
    // bootstrap-server
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils("kafka.broker.list"))
    // kv
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    //acks
    //    ProducerConfig.ACKS_CONFIG

    //  创建 kafka 生产者对象
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)
    producer
  }

  private var producer: KafkaProducer[String, String] = kafkaProducer()


  /**
   * 生产fakfa数据
   */
  def send(topic: String, msg: String) = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  //生产 指定 key
  def send(topic: String, key: String, msg: String) = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }


  /**
   * 刷写缓冲区
   */
  def flush() = {
    if (producer != null) producer.flush()
  }

  //关闭生产者对象
  def close() = {
    if (producer != null) producer.close()
  }

}
