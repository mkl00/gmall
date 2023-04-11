package com.app

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.bean.{OrderDetail, OrderInfo, OrderWide}
import com.util.{MyESUtils, MyKafkaUtils, MyRedisUtils, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * 1.配置流环境
 * 2.从redis中读取offset
 * 3.读取kafka数据
 * 4.获取结束端offset
 * 5.数据处理
 *    5.1 转换结构
 *    5.2 维度关联
 *    5.3 双流join
 * 6. 写入ES
 * 7. 提交offset
 */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    //    1.配置流环境
    val conf = new SparkConf().setAppName("dwd_order_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //  2.从redis中读取offset
    //order_info",
    val orderInfoTopic = "DWD_ORDER_INFO_I"
    val orderInfoGroupId = "DWD_ORDER_INFO:GROUP"
    val orderInfoOffset: Map[TopicPartition, Long] = OffsetManagerUtil.readOffset(orderInfoTopic, orderInfoGroupId)

    //"order_detail
    val orderDetailTopic = "DWD_ORDER_DETAIL_I"
    val orderDateilGroup = "DWD_ORDER_DATEIL:GROUP"
    val orderDetailOffset: Map[TopicPartition, Long] = OffsetManagerUtil.readOffset(orderDetailTopic, orderDateilGroup)


    //3.读取kafka数据
    //order_info
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffset != null && orderInfoOffset.nonEmpty) {
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopic, orderInfoGroupId, orderInfoOffset)
    } else {
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopic, orderInfoGroupId)
    }

    //order_detail
    var orderDateilKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffset != null && orderDetailOffset.nonEmpty) {
      orderDateilKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopic, orderDateilGroup, orderDetailOffset)
    } else {
      orderDateilKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopic, orderDateilGroup)
    }

    //4.获取结束端offset
    //order_info
    var orderInfoOffsetRange: Array[OffsetRange] = null
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //order_detail
    var orderDateilOffsetRange: Array[OffsetRange] = null
    val orderDateilOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDateilKafkaDStream.transform(
      rdd => {
        orderDateilOffsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5.数据处理
    //  5.1 类型转换
    //order_info
    val orderInfoJsonObj: DStream[OrderInfo] = orderInfoOffsetDStream.map(
      rdd => {
        val value: String = rdd.value()
        val info: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        info
      }
    )
    //    orderInfoJsonObj.print(10)

    //order_detail
    val orderDetailJsonObj: DStream[OrderDetail] = orderDateilOffsetDStream.map(
      rdd => {
        val value: String = rdd.value()
        val detail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        detail
      }
    )
    //    orderDetailJsonObj.print(10)

    //  5.2  维度关联，维度合并(补充用户年龄性别 、 地区信息)
    // orderInfo
    val orderInfoMap: DStream[OrderInfo] = orderInfoJsonObj.mapPartitions(
      orderInfojIter => {

        var listOrderInfo: ListBuffer[OrderInfo] = new ListBuffer[OrderInfo]
        val jedis = MyRedisUtils.getRedisPool()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

        for (orderInfo <- orderInfojIter) {

          // todo 补充用户信息
          val userInfoKey = s"DIM:USER_INFO:${orderInfo.user_id}"
          val str: String = jedis.get(userInfoKey)
          val orderInfoJsonObj: JSONObject = JSON.parseObject(str)

          // 获取性别
          val gender: String = orderInfoJsonObj.getString("gender")
          orderInfo.user_gender = gender

          // 获取年龄
          val birthday: String = orderInfoJsonObj.getString("birthday")
          val birthdayDate: LocalDate = LocalDate.parse(birthday)
          val isnow = LocalDate.now()
          val period = Period.between(birthdayDate, isnow)
          orderInfo.user_age = period.getYears

          //todo 补充日期字段
          val dateTimeArr = orderInfo.create_time.split(" ")
          orderInfo.create_date = dateTimeArr(0)
          orderInfo.create_hour = dateTimeArr(1).split(":")(0)

          // todo 关联地区维度
          val provinceKey = s"DIM:BASE_PROVINCE:" + orderInfo.province_id
          val provinceString: String = jedis.get(provinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceString)
          orderInfo.province_name = provinceJsonObj.getString("name")
          orderInfo.province_area_code = provinceJsonObj.getString("area_code")
          orderInfo.province_3166_2_code = provinceJsonObj.getString("iso_3166_2")
          orderInfo.province_iso_code = provinceJsonObj.getString("iso_code")

          listOrderInfo.append(orderInfo)
        }

        jedis.close()
        listOrderInfo.toIterator
      }
    )

//    orderInfoMap.print()

    //order_detail

    //  5.3 双流join
    // 内连接 join 结果区交集
    // 外连接 join
    //  左外连接  右外连接   全外连接
    //由于两个流的数据是独立保存，独立消费，很有可能同一业务的数据，分布在不同的
    //批次。因为 join 算子只 join 同一批次的数据。如果只用简单的 join 流方式，会丢失掉不同
    //批次的数据。
    // todo 如果要做 Join 操作, 必须是 DStream[K,V] 和 DStream[K,V]
    //order_info
    val orderInfoKV: DStream[(Long, OrderInfo)] = orderInfoMap.map(orderInfo => (orderInfo.id, orderInfo))
    //order_detail
    val orderDetailKV: DStream[(Long, OrderDetail)] = orderDetailJsonObj.map(orderDetail => (orderDetail.id, orderDetail))
    //join 只能实现统一批次的数据进行 Join,如果有数据延迟，延迟的数据就不能 join 成功， 就会有数据丢失.
    //    val joinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoKV.join(orderDetailKV)
    //    joinDStream.print()

    //通过状态或者缓存来解决。
    // 使用fullouterjoin 保证join成功和或者失败的数据都出现在结果中
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoKV.fullOuterJoin(orderDetailKV)
    //    orderJoinDStream.print()

    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.mapPartitions(
      orderjoinIter => {
        val orderWiedeList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        val jedis: Jedis = MyRedisUtils.getRedisPool()
        for (orderjoin <- orderjoinIter) {

          // orderinfo有   orderdetail有
          if (orderjoin._2._1 != None) {
            // 取出orderinfo
            val orderInfo: OrderInfo = orderjoin._2._1.get
            if (orderjoin._2._2 != None) {
              val orderDetail: Option[OrderDetail] = orderjoin._2._2
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail.get)
              // 放入到结果集中
              orderWiedeList.append(orderWide)
            }

            // 读缓存
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size() > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetailobj: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                val orderWide: OrderWide = new OrderWide(orderInfo, orderDetailobj)
                // 放入到结果集中
                orderWiedeList.append(orderWide)
              }
            }

            // orderinfo有   orderdetail没有
            // 写缓存
            //类型：String
            // key ： ORDERJOIN:ORDER_INFO:ID
            // value : JSON
            // 写入API：set
            // 读取API：smember
            // 过期时间：24小时
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
            jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))

          } else {
            val orderDetail: OrderDetail = orderjoin._2._2.get
            // orderinfo没有   orderdetail有
            // 读缓存
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO${orderDetail.id}"
            val orderInfoStr: String = jedis.get(redisOrderInfoKey)
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
            if (orderInfo != null) {
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              // 放入到结果集中
              orderWiedeList.append(orderWide)
            } else {
              // 写缓存
              // 类型：set
              // key：ORDERJOIN:ORDER_DETAIL:id
              // value：json。。。
              // 写入API :  sadd
              // 读取APO： smembers
              // 过期：24小时
              val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
              val orderDetailJson: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
              jedis.sadd(redisOrderDetailKey, orderDetailJson)
              jedis.expire(redisOrderDetailKey, 24 * 3600)
            }
          }
        }
        jedis.close()
        orderWiedeList.iterator
      }
    )

    orderWideDStream.cache()
    orderWideDStream.print()

    // todo 写入es
    orderWideDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          orderWideIter => {
            val orderWides: List[(String, OrderWide)] = orderWideIter.toList.map(orderWide => (orderWide.detail_id.toString, orderWide))
            if (orderWides.size > 0) {
              val ts: String = orderWides(0)._2.create_date

              MyESUtils.bulkSaveIdempotent(orderWides, s"gmall_order_wide_$ts")
            }
          }
        )
        // todo 提交offset
        OffsetManagerUtil.savnOffset(orderDetailTopic, orderDateilGroup, orderDateilOffsetRange)
        OffsetManagerUtil.savnOffset(orderInfoTopic, orderInfoGroupId, orderInfoOffsetRange)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}