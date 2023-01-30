package app

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import bean.{AlertInfo, EventLog, StartUpLog}
import com.alibaba.fastjson.JSON
import common.Constant
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import utils.{MyKafkaUtil, PropertiesUtil, RedisUtil}

import scala.util.control.Breaks._

/**
 * @Auther: wxf
 * @Date: 2023/1/29 16:22:01
 * @Description: AlertApp   需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
 *               并且在登录到领劵过程中没有浏览商品。
 *               同时达到以上要求则产生一条预警日志。
 *               同一设备，每分钟只记录一次预警。
 * @Version 1.0.0
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))

    val (kafkaStream, zkClient, zkTopicPath): (InputDStream[ConsumerRecord[String, String]], ZkClient, String) =
      MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_EVENT, PropertiesUtil.getProperty("kafka.event.group.id"))

    //通过rdd转换得到偏移量的范围
    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

    // 封装样例类，获取 offsetRanges
    val transformStream: DStream[EventLog] = kafkaStream
      .transform(rdd => {
        //得到该RDD对应kafka消息的offset,该RDD是一个KafkaRDD，所以可以获得偏移量的范围
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      })
      .mapPartitions(it => {
        val date: Date = new Date()
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val dateFormat1: SimpleDateFormat = new SimpleDateFormat("HH")
        it.map(x => {
          val event: String = x.value()
          val log: EventLog = JSON.parseObject(event, classOf[EventLog])
          val ts: Long = log.ts
          date.setTime(ts)
          log.logDate = dateFormat.format(date)
          log.logHour = dateFormat1.format(date)
          log
        })
      })
      .window(Minutes(5), Seconds(6)) // 1.设置 5分钟窗口,6秒输出一次

    //    transformStream.print()

    //     保存偏移量
    transformStream.foreachRDD(rdd => {
      val result: RDD[(Boolean, AlertInfo)] = rdd.map(event => (event.mid, event))
        .groupByKey() // 2.按照 同一个 mid 进行分组
        .map { // 3.三个不同账号登录；4.领取优惠券；5.并且没有浏览商品
          case (mid, events) =>
            // a.存储登录过的所有用户(账号)；统计当前设备在最近5分钟登录过的所有用户
            val uidSet: util.HashSet[String] = new util.HashSet[String]()
            // b.存储5分钟内当前设备所有的事件
            val eventList: util.ArrayList[String] = new util.ArrayList[String]()
            // c.存储领取优惠券对应的那些产品id
            val itemSet: util.HashSet[String] = new util.HashSet[String]()
            // d.表示是否点击商品 默认没有点击
            var isClickItem: Boolean = false

            breakable {
              events.foreach(log => {
                eventList.add(log.eventId) // 存储所有事件
                log.eventId match {
                  case "coupon" =>
                    uidSet.add(log.uid) // 存储领取优惠券的用户id
                    itemSet.add(log.itemId) // 优惠券对应的商品id
                  case "clickItem" => // 表示 该事件是浏览商品
                    //   uidSet.clear()  // 清空Set 是否也可以判断是否需要报警 ????
                    // 只要有一次浏览商品，5分钟内就不应该参数预警信息
                    isClickItem = true
                    break
                  case _ => // 其他事件不做处理
                }
              })
            }
            // (是否需要预警，预警信息)
            (uidSet.size() >= 3 && !isClickItem, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
        }

      // 6.同一设备每分钟只预警一次(同一设备, 每分钟只向 es 写一次记录)

      println("=================")
      result.foreach(println)

      for (o <- offsetRanges) {
        val zkPath: String = s"${zkTopicPath}/${o.partition}"
        ZkUtils(zkClient, false).updatePersistentPath(zkPath, o.untilOffset.toString)
      }

    })

    ssc.start()
    ssc.awaitTermination()

  }
}
