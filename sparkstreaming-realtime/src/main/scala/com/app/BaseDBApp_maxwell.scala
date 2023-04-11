package com.app

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.util.{MyKafkaUtils, MyRedisUtils, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * 业务数据分流
 * 1.读取偏移量
 * 2.接受kafka数据
 * 3.提取偏移量位置
 * 4。转换结构
 * 5.分流处理
 *    5.1 事实数据分流 -》kafka
 *    5.2 维度数据分流 -》 redis
 * 6.提交偏移量
 */
object BaseDBApp_maxwell {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("base_db_app").setMaster("local[4]")
    val ssc = new StreamingContext(sc, Seconds(5))

    val topic = "ODS_BASE_DB_M"
    val groupId = "base_db_group"

    //1.读取偏移量
    val offsets: Map[TopicPartition, Long] = OffsetManagerUtil.readOffset(topic, groupId)

    var kafkaDStream: DStream[ConsumerRecord[String, String]] = null
    // 判断偏移量是否为空
    if (offsets != null && offsets.nonEmpty) {
      //2.接受kafka数据
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId)
    }

    // 3.提取偏移量结束位置
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform( // driver
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //    4。转换结构
    val valueJsonObj: DStream[JSONObject] = offsetRangesDStream.map(
      record => {
        // 获取 record中的value，value就是日志数据
        val str: String = record.value()
        println(str)
        val JsonObject: JSONObject = JSON.parseObject(str)
        JsonObject
      }
    )

//    valueJsonObj.print(10)

    //5.分流处理
    // 5.1 事实数据
    //    val factTables: Array[String] = Array[String]("order_info", "order_detail")
    // 5.2 维度数据
    //    val dimTables: Array[String] = Array[String]("user_info", "base_province")

    //Redis连接写到哪里???
    // foreachRDD外面:  driver  ，连接对象不能序列化，不能传输
    // foreachRDD里面, foreachPartition外面 : driver  ，连接对象不能序列化，不能传输
    // foreachPartition里面 , 循环外面：executor ， 每分区数据开启一个连接，用完关闭.
    // foreachPartition里面,循环里面:  executor ， 每条数据开启一个连接，用完关闭， 太频繁。

    valueJsonObj.foreachRDD(
      rdd => {
        //如何动态配置表清单???
        // 将表清单维护到redis中，实时任务中动态的到redis中获取表清单.
        // 类型: set
        // key:  FACT:TABLES   DIM:TABLES
        // value : 表名的集合
        // 写入API: sadd
        // 读取API: smembers
        // 过期: 不过期

        val jedis: Jedis = MyRedisUtils.getRedisPool()
        val dimTableKey: String = "DIM:TABLES"
        val factTableKey: String = "FACT:TABLES"
        // 从redis 中读取订单表
        val dimTables: util.Set[String] = jedis.smembers(dimTableKey)
        val factTables: util.Set[String] = jedis.smembers(factTableKey)
        println("检查事实表: " + factTables)
        println("检查维度表: " + dimTables)
        // 做成广播变量
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
        jedis.close()

        // 只要有一个批次执行一个，且在 executor 中执行的代码，就需要用foreachpartition
        rdd.foreachPartition(
          jsonObjIter => {
            // 获取redis连接
            val jedis: Jedis = MyRedisUtils.getRedisPool()
            for (jsonObj <- jsonObjIter) {
              //提取操作类型
              val optType: String = jsonObj.getString("type")
              val opt: String = optType match {
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null // DDL 操 作 ， 例 如 : CREATE ALTER TRUNCATE ....
              }

              //判断操作类型: 1. 明确什么操作  2. 过滤不感兴趣的数据
              if (opt != null) {
                // 提取表名
                val tableName: String = jsonObj.getString("table")
                // 提取修改后的数据
                val dataJsonObj = jsonObj.getJSONObject("data")

                //事实表数据
                if (factTablesBC.value.contains(tableName)) {
                  // 提取数据
                  // 拆分到指定的主题
                  // topic => DWD_[TABLE_NAME]_[I/U/D]
                  val dwdTopicName = s"DWD_${tableName.toUpperCase}_${opt}"
                  println("事实表操作---------------------")
                  //val key: String = dataJsonObj.getString("id")
                  //发送 kafka
                  MyKafkaUtils.send(dwdTopicName, dataJsonObj.toJSONString)
                }

                // todo 维度表处理
                if (dimTablesBC.value.contains(tableName)) {
                  //维度数据
                  // 类型 : string  hash
                  //        hash ： 整个表存成一个hash。 要考虑目前数据量大小和将来数据量增长问题 及 高频访问问题.
                  //        hash :  一条数据存成一个hash.
                  //        String : 一条数据存成一个jsonString.
                  // key :  DIM:表名:ID
                  // value : 整条数据的jsonString
                  // 写入API: set
                  // 读取API: get
                  // 过期:  不过期

                  //提取数据中的id
                  val id: String = dataJsonObj.getString("id")
                  val redisKey: String = s"DIM:${tableName.toUpperCase}:$id"
                  println("维度表操作---------------------")
                  jedis.set(redisKey, dataJsonObj.toJSONString)
                }
              }
            }
            //关闭redis连接
            jedis.close()
            //刷新Kafka缓冲区
            MyKafkaUtils.flush()
          }
        )
        // 提交offset
        OffsetManagerUtil.savnOffset(topic, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
