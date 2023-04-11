package com.app

import java.lang
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bean.{DauInfo, PageLog}
import com.util.{MyBeanUtils, MyESUtils, MyKafkaUtils, MyRedisUtils, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * 1.准备实时环境
 * 2.从redis中读取offset
 * 3.从kafka中消费数据
 * 4.提取offset结束点
 * 5.处理数据
 *  5.1转换数据结构
 *  5.2 去重
 *  5.3 维度关联
 *  6.写入ES
 * 7.提交offset
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    //1.准备实时环境
    val conf = new SparkConf().setMaster("local[4]").setAppName("dwd-dau-app")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //2.从redis中读取offset
    val topic = "DWD_PAGE_LOG"
    val groupId = "DWD_DAU_GROUP"
    val offset: Map[TopicPartition, Long] = OffsetManagerUtil.readOffset(topic, groupId)

    //3. 从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offset != null && offset.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId, offset)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId)
    }

    // 4.提取offset结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5. 处理数据
    //  5.1 数据转换   Json——>bean
    val pageLogDStream: DStream[PageLog] = offsetDStream.map(
      rdd => {
        val str: String = rdd.value()
        val pageLog = JSON.parseObject(str, classOf[PageLog])
        pageLog
      }
    )

    //    pageLogDStream.print(10)

    //    pageLogDStream.cache()
    //    pageLogDStream.foreachRDD(
    //      rdd => {
    //        println("自我审查前：" + rdd.count())
    //      }
    //    )

    // 5.2 去重操作  ： 过滤掉last_page_id不为空的数据
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      data => {
        data.last_page_id == null
      }
    )

    //    filterDStream.cache()
    //    filterDStream.foreachRDD(
    //      rdd => {
    //        println("自我审查后：" + rdd.count())
    //        println("---------------------------------")
    //      }
    //    )

    // 第三方审查：通过redis将当日活跃的mid维护起来，自我审查后的每条数据需要redis中进行比对去重
    // redis 中如何维护日活状态
    // 类型： set
    // key： U
    // value:
    // 读取的API ：samn
    // 写入的API: sadd
    // 过不过期：  过期一天
    //    filterDStream.filter()   这个方法不适用，因为每来一条数据就会进行一次redis连接
    val mapRedisDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        // 第三方审查前
        val pageLogIterList: List[PageLog] = pageLogIter.toList
        println("第三方审查前:" + pageLogIterList.size)

        // jedis 连接
        val jedis: Jedis = MyRedisUtils.getRedisPool()
        // 存储要的数据
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        // 时间格式转换
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

        for (pageLog <- pageLogIterList) {
          // 提取每条数据中的mid（我们日活统计基于mid，也可以基于uid）
          val mid: String = pageLog.mid
          // 获取日期，因为我们要测试不同天的数据，所以不能直接获取系统时间
          val ts: Long = pageLog.ts
          val date: Date = new Date(ts)
          val dateStr: String = sdf.format(date)

          val redisDauKey = s"DAU:$dateStr"
          val isNew: lang.Long = jedis.sadd(redisDauKey, mid)
          jedis.expire(redisDauKey, 24 * 3600)
          // 判断数据否是添加到redis里，返回0说明，数据已经存在
          // 判断包含和写入实现了原子性操作
          if (isNew == 1L) { //说明数据添加成功
            pageLogs.append(pageLog)
          }

        }
        println("第三方审查后:" + pageLogs.size)

        jedis.close()
        pageLogs.iterator
      }
    )
    //    mapDStream.print()

    //  5.3 维度关联
    val dauInfoDStream: DStream[DauInfo] = mapRedisDStream.mapPartitions(
      pageLogIter => {
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        var dauInfoList: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val jedis: Jedis = MyRedisUtils.getRedisPool()
        for (pageLog <- pageLogIter) {
          var dauInfo: DauInfo = new DauInfo()
          // 1.将pageLog中的所有字段，拷贝到DauInfo中
          // 笨办法：将pagelog中的字段挨个提取，赋值给duainfo中对应的字段
          // 好办法：通过对象copy来完成
          MyBeanUtils.copyProperties(pageLog, dauInfo) // 反射
          // 2 补充维度
          //todo  2.1 用户信息维度
          // 用户信息关联
          val dimUserKey = s"DIM:USER_INFO:${pageLog.user_id}"
          val userInfoJson: String = jedis.get(dimUserKey)
          val userinfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          // 提取生日
          val birthday: String = userinfoJsonObj.getString("birthday")
          // 提取性别
          val gender: String = userinfoJsonObj.getString("gender")
          //生日处理为年龄
          var age: String = null
          if (birthday != null) {
            // 闰年无误差
            val birthdayDate: LocalDate = LocalDate.parse(birthday)
            val nowDate: LocalDate = LocalDate.now()
            val period: Period = Period.between(birthdayDate, nowDate)
            val years: Int = period.getYears
            age = years.toString
          }
          dauInfo.user_gender = gender
          dauInfo.user_age = age

          //todo  2.1 地区信息维度
          val dimProvinceKey = s"DIM:BASE_PROVINCE:${pageLog.province_id}"
          val dimProvinceJson: String = jedis.get(dimProvinceKey)
          val dimProvinceJsonObj: JSONObject = JSON.parseObject(dimProvinceJson)
          dauInfo.province_name = dimProvinceJsonObj.getString("name")
          dauInfo.province_iso_code = dimProvinceJsonObj.getString("iso_code")
          dauInfo.province_3166_2 = dimProvinceJsonObj.getString("3166_2")
          dauInfo.province_area_code = dimProvinceJsonObj.getString("area_code")

          //todo  2.1 日期信息维度
          val ts: Long = pageLog.ts
          val date: Date = new Date(ts)
          val dateFormat: String = sdf.format(date)
          val dts: Array[String] = dateFormat.split(" ")
          val dt = dts(0)
          val hr = dts(1)
          dauInfo.dt = dt
          dauInfo.hr = hr


          dauInfoList.append(dauInfo)
        }
        jedis.close()
        dauInfoList.toIterator
      }
    )

    dauInfoDStream.cache()
    dauInfoDStream.print()

    // TODO 8.写入 ES
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            // 因为是日活宽表，一天内mid是不应该重复的
            // 转换结构数据，保证数据幂等写入
            val dauInfos: List[(String, DauInfo)] = dauInfoIter.toList.map(dauInfo => (dauInfo.mid, dauInfo))
            if (dauInfos.size > 0) {
              // 从数据中获取日期，拼接 ES的索引名
              val dauInfoT: (String, DauInfo) = dauInfos(0)
              val dt: String = dauInfoT._2.dt
              MyESUtils.bulkSaveIdempotent(dauInfos, s"gmall_dau_info_$dt")
            }
          }
        )
        // TODO 9.提交偏移量
        OffsetManagerUtil.savnOffset(topic, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
