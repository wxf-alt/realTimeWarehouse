package app

import java.text.SimpleDateFormat

import bean.StartUpLog
import com.alibaba.fastjson.{JSON, JSONObject}
import common.Constant
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import utils.{MyKafkaUtil, PropertiesUtil, RedisUtil}
import java.util
import util.{Date, Properties}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}


/**
 * @Auther: wxf
 * @Date: 2023/1/13 15:11:45
 * @Description: DauApp  统计日活
 * @Version 1.0.0
 */
object DauApp {

  var logDate: String = _

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 1.从 Kafka 读取数据
    // 读取启动日志 topic
    //    val (kafkaStream, zkClient, zkTopicPath): (InputDStream[ConsumerRecord[String, String]], ZkClient, String) = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_STARTUP)
    val (kafkaStream, zkClient, zkTopicPath): (InputDStream[ConsumerRecord[String, String]], ZkClient, String) = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_STARTUP, PropertiesUtil.getProperty("kafka.dau.group.id"))

    //通过rdd转换得到偏移量的范围
    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

    kafkaStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        //得到该RDD对应kafka消息的offset,该RDD是一个KafkaRDD，所以可以获得偏移量的范围
        //不使用transform可以直接在foreachRDD中得到这个RDD的偏移量，这种方法适用于DStream不经过任何的转换，
        //直接进行foreachRDD，因为如果transformation了那就不是KafkaRDD了，就不能强转成HasOffsetRanges了，从而就得不到kafka的偏移量了
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        // 转换为 样例类
        val dataRDD: RDD[StartUpLog] = kafkaRDD
          .map(_.value())
          .mapPartitions(it => {
            val date: Date = new Date()
            val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val dateFormat1: SimpleDateFormat = new SimpleDateFormat("HH")
            it.map(x => {
              val log: StartUpLog = JSON.parseObject(x, classOf[StartUpLog])
              val ts: Long = log.ts
              date.setTime(ts)
              log.logDate = dateFormat.format(date)
              log.logHour = dateFormat1.format(date)
              logDate = log.logDate
              //              println("StartUpLog：" + log)
              log
            })
          })

        // 2.过滤得到日活明细
        // 需要借助第三方工具 进行 去重 使用 Redis
        val jedis: Jedis = RedisUtil.getClient()
        val redisKey: String = Constant.TOPIC_STARTUP + "_" + logDate
        // 2.1 从 redis 中读取已经启动的设备
        val midSet: util.Set[String] = jedis.smembers(redisKey)
        jedis.close()
        // 封装 样例类
        val uidSetBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)

        val filterStartupLogRDD: RDD[StartUpLog] = dataRDD
          // 进行去重操作
          .groupBy(_.mid)
          .map({
            case (_, it) => it.minBy(_.ts)
          })
          .filter(startupLog => {
            val mids: util.Set[String] = uidSetBC.value
            // 2.2 把已经启动的设备过滤掉, rdd中只留下那些在redis中不存在的记录
            !mids.contains(startupLog.mid)
          })

        // 2.3 把第一次启动的设备保存到 redis 中
        filterStartupLogRDD.foreachPartition(x => {
          val jedis: Jedis = RedisUtil.getClient()
          x.foreach(log => {
            println("log：" + log)
            val key: String = Constant.TOPIC_STARTUP + "_" + log.logDate
            jedis.sadd(key, log.mid)
            log
          })
          jedis.close()
        })

        //        // 3.将日活明细保存到 HBase 中
        //        // 需要先将表在 phoenix 中创建好
        //        /*
        //        create table gmall_dau(
        //                        mid varchar,
        //                        uid varchar,
        //                        appid varchar,
        //                        area varchar,
        //                        os varchar,
        //                        channel varchar,
        //                        logType varchar,
        //                        version varchar,
        //                        logDate varchar,
        //                        logHour varchar,
        //                        ts bigint
        //                        CONSTRAINT dau_pk PRIMARY KEY (mid,logDate));
        //         */
        //        import org.apache.phoenix.spark._
        //        filterStartupLogRDD.saveToPhoenix("gmall",
        //          Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        //          zkUrl = Some("nn1.hadoop,nn2.hadoop,s1.hadoop:2181")
        //        )

        import sparkSession.implicits._
        // 嫌麻烦,还要安装 Hadoop - HBase集群 和 phoenix
        //    所以使用 MySql 存储 日活数据
        val startupLogDS: Dataset[StartUpLog] = filterStartupLogRDD.toDS()
        val properties: Properties = new Properties()
        properties.setProperty("user", PropertiesUtil.getProperty("mysql.user"))
        // 4.将数据写到Mysql中
        properties.setProperty("password", PropertiesUtil.getProperty("mysql.password"))

        startupLogDS.write.mode(SaveMode.Append).jdbc(PropertiesUtil.getProperty("mysql.url"), "gmall", properties)

        for (o <- offsetRanges) {
          val zkPath: String = s"${zkTopicPath}/${o.partition}"
          //将该 partition 的 offset 保存到 zookeeper
          //          println(s"${zkPath}__${o.untilOffset.toString}")
          ZkUtils(zkClient, false).updatePersistentPath(zkPath, o.untilOffset.toString)
        }

      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
