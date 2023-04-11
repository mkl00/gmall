package com.util

import java.util.ResourceBundle

/**
 * 解析配置类
 */
object PropertiesUtils {
  private val bundle: ResourceBundle = {
    ResourceBundle.getBundle("config")
  }

  def apply(key: String): String = {
    bundle.getString(key)
  }

  def main(args: Array[String]): Unit = {
//    print(PropertiesUtils("kafka.broker.list"))
    print(PropertiesUtils("redis.port"))
  }
}
