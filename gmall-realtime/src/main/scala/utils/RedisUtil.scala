package utils

import java.{lang, util}

import redis.clients.jedis.Jedis

/**
 * @Auther: wxf
 * @Date: 2023/1/16 11:06:22
 * @Description: RedisUtil
 * @Version 1.0.0
 */
object RedisUtil {

  private val host: String = PropertiesUtil.getProperty("redis.host")
  private val post: Int = PropertiesUtil.getProperty("redis.port").toInt

  def getClient(): Jedis = {
    val jedisClient: Jedis = new Jedis(host, post, 60 * 1000)
    jedisClient.connect()
    //    jedisClient.select(0)
    jedisClient
  }

  //  def main(args: Array[String]): Unit = {
  //    val jedis: Jedis = getClient()
  //    //    val set: util.Set[String] = jedis.smembers("topic_startup_2023-01-16")
  //    val long: lang.Long = jedis.sadd("test_topic", "as", "rd")
  //    println(long)
  //  }

}
