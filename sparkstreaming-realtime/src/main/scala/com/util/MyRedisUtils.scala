package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * reids 工具类  用于连接redis连接配置，操作redis
 */
object MyRedisUtils {

  var jedisPool: JedisPool = null

  def getRedisPool(): Jedis = {
    if (jedisPool == null) {

      // redis 配置
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(8000) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      val host: String = PropertiesUtils("redis.host")
      val port: String = PropertiesUtils("redis.port")

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    println(jedisPool)
    val jedis: Jedis = getRedisPool
  }
}
