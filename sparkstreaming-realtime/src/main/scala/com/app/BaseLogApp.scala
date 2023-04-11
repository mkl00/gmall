package com.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.util.{MyKafkaUtils, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 消费分流
 * 1. 接受kafka数据
 *
 * 2.转换数据结构
 * 通用的数据结构:map或者JsonObject
 * 专用的数据结构:bean
 *
 * 3.分流,将数据差分到不同的主题中
 * 启动主题: DWD_START_LOG
 * 页面访问主题: DWD_PAGE_LOG
 * 页面动作主题:DWD_PAGE_ACTION
 * 页面曝光主题:DWD_PAGE_DISPLAY
 * 错误主题:DWD_ERROR_INFO
 */
object BaseLogApp {

  def main(args: Array[String]): Unit = {
    // 创建配置对象
    //todo 注意并行度与kafka中topic的分区个数的对应关系
    var sparkConf: SparkConf = new SparkConf().setAppName("base_log_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //    原始主题
    val ods_base_topic = "ODS_BASE_LOG"

    //    消费组
    val group_id = "ods_base_log_group"

    // todo 从redis中读取  offset
    val offset: Map[TopicPartition, Long] = OffsetManagerUtil.readOffset(ods_base_topic, group_id)

    // 判断是否能读取到
    var kafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if (offset != null && offset.nonEmpty) {
      //redis中有数据
      // 1.接受kafka数据流
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, ods_base_topic, group_id, offset)
    } else {
      // redis中没有数据
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, ods_base_topic, group_id)

    }

    //    1.接受kafka的数据流
    //    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDStream(ssc, ods_base_topic, group_id, offset)

    //    kafkaDStream.map(_.value()).print(3)

    // todo 在数据转换前，从当前流中提取出offset
    var offsetRange: Array[OffsetRange] = null //driver
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform( // 每批执行一次
      rdd => {
        offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //    2.数据转换
    val valueJsonDStream: DStream[JSONObject] = offsetRangesDStream.map(
      record => {
        // 获取 record中的value，value就是日志数据
        val str: String = record.value()
        println(str)
        //        转换成Json对象
        val jsonObject: JSONObject = JSON.parseObject(str)
        // 返回
        jsonObject
      }
    )
    valueJsonDStream.print(10)

    // 3.2 分流
    //    日志数据
    //      页面访问数据
    //        公共字段
    //        页面数据
    //        曝光数据
    //        事件数据
    //        错误数据
    //      启动数据
    //        公共字段
    //        启动数据
    //        错误数据

    //    启动主题: DWD_START_LOG
    val dwd_start_log = "DWD_START_LOG"

    //    页面访问主题: DWD_PAGE_LOG
    val dwd_page_log = "DWD_PAGE_LOG"

    //    页面动作主题:DWD_PAGE_ACTION
    val dwd_dwd_page_action = "DWD_PAGE_ACTION"

    //    页面曝光主题:DWD_PAGE_DISPLAY
    val dwd_page_display = "DWD_PAGE_DISPLAY"

    //    错误主题:DWD_ERROR_INFO
    val dwd_error_info = "DWD_ERROR_INFO"


    // 分流规则
    // 错误数据：不做任何拆分，直接发送到，kafka topic中
    // 页面数据：拆分成  页面访问、曝光、事件，  分别发送到对应的topic
    // 启动数据：不做任何拆分，直接发送到，kafka topic中数据：
    valueJsonDStream.foreachRDD(
      rdd => {
        rdd.foreach(
          jsonobj => {
            // 分流过程
            // 分流错误数据
            val errobj: JSONObject = jsonobj.getJSONObject("err")
            if (errobj != null) {
              // 将错误数据发送到 DWD_ERROR_INFO
              MyKafkaUtils.send(dwd_error_info, errobj.toJSONString)
            } else {
              // 提取公共字段
              val commonObj: JSONObject = jsonobj.getJSONObject("common")
              val ar = commonObj.getString("ar")
              val ba = commonObj.getString("ba") // 品牌
              val ch = commonObj.getString("ch") // 渠道
              val is_new = commonObj.getString("is_new") // 是否是新的
              val md = commonObj.getString("md") // 型号
              val mid = commonObj.getString("mid") //  设备id
              val os = commonObj.getString("os") // 操作系统
              val uid = commonObj.getString("uid") // 用户id
              val vc = commonObj.getString("vc") //  版本

              // 提取时间戳
              val ts = jsonobj.getLong("ts")

              //分流页面日志  提取页面数据  page
              val pageJson = jsonobj.getJSONObject("page")
              if (pageJson != null) {
                //提取字段
                val pageId: String = pageJson.getString("page_id")
                val pageItem: String = pageJson.getString("item")
                val pageItemType: String = pageJson.getString("item_type")
                val lastPageId: String = pageJson.getString("last_page_id")
                val duringTime: Long = pageJson.getLong("during_time")

                //封装 bean
                val pageLog = PageLog(mid, uid, ar, ch, is_new, md, os, vc, pageId, lastPageId, pageItem, pageItemType, duringTime, ts)

                // 发送kafka
                MyKafkaUtils.send(dwd_page_log, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                //分流  动作日志
                val actionsArrayObj = jsonobj.getJSONArray("actions")
                if (actionsArrayObj != null && actionsArrayObj.size() > 0) {
                  for (i <- 0 until actionsArrayObj.size()) {
                    val actionObj: JSONObject = actionsArrayObj.getJSONObject(i)
                    val action_id = actionObj.getString("action_id")
                    val actioniItem = actionObj.getString("item")
                    val actioniItem_tyep = actionObj.getString("item_tyep")
                    val actionTs = actionObj.getLong("ts")

                    // 封装bean
                    val pageActionLog = PageActionLog(mid, uid, ar, ch, is_new, md, os, vc, pageId, lastPageId, pageItem, pageItemType, duringTime, action_id, actioniItem, actioniItem_tyep, actionTs)

                    //发送 Kafka
                    MyKafkaUtils.send(dwd_dwd_page_action, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                  }
                }

                // 分流曝光日志
                val displaysArray = jsonobj.getJSONArray("displays")
                if (displaysArray != null && displaysArray.size() > 0) {
                  for (i <- 0 until displaysArray.size()) {
                    val displayObj: JSONObject = displaysArray.getJSONObject(i)
                    val display_type = displayObj.getString("query")
                    val displayItem = displayObj.getString("item")
                    val displayItemType = displayObj.getString("item_type")
                    val order = displayObj.getLong("order")
                    val pos_id = displayObj.getLong("pos_id")

                    // 封装bean
                    val pageDisplayLog = PageDisplayLog(mid, uid, ar, ch, is_new, md, os, vc, pageId, lastPageId, pageItem, pageItemType, duringTime, display_type, displayItem, displayItemType, order, pos_id, ts)

                    // 发送到 kafka DWD_PAGE_DISPLAY
                    MyKafkaUtils.send(dwd_page_display, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                  }
                }
              }

              //  分流启动数据
              val startObj: JSONObject = jsonobj.getJSONObject("start")
              if (startObj != null) {
                val entry: String = startObj.getString("entry")
                val loading_time: Long = startObj.getLong("loading_time")
                val open_ad_id: Long = startObj.getLong("open_ad_id")
                val open_ad_ms: Long = startObj.getLong("open_ad_ms")
                val open_ad_skip_ms: Long = startObj.getLong("open_ad_skip_ms")

                val ts = startObj.getLong("ts")
                val startLog = StartLog(mid, uid, ar, ch, is_new, md, os, vc, entry, open_ad_id, loading_time, open_ad_ms, open_ad_skip_ms, ts)
                MyKafkaUtils.send(dwd_start_log, JSON.toJSONString(startLog, new SerializeConfig(true)))
              }
            }
          }
        )

        // foreachRDD 里边，driver中执行，一个批次执行一次
        // 提交偏移量
        OffsetManagerUtil.savnOffset(ods_base_topic, group_id, offsetRange)
      }
    )
    // foreahRDD 外面 driver 中执行，一次启动执行一次

    ssc.start()
    ssc.awaitTermination()

  }
}

