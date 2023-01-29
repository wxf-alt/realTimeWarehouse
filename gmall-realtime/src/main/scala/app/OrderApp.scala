package app

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bean.OrderInfo
import com.alibaba.fastjson.JSON
import common.Constant
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{MyKafkaUtil, PropertiesUtil}

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 1.从 Kafka 读取数据
    // 读取启动日志 topic
    val (kafkaStream, zkClient, zkTopicPath): (InputDStream[ConsumerRecord[String, String]], ZkClient, String) = MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO, PropertiesUtil.getProperty("kafka.order.group.id"))

    //通过rdd转换得到偏移量的范围
    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

    kafkaStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        // 转换为 样例类
        val orderInfoRdd: RDD[OrderInfo] = kafkaRDD.map(_.value())
          .mapPartitions(it => {
            val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val dateFormat1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val dateFormat2: SimpleDateFormat = new SimpleDateFormat("HH")
            it.map(x => {
              val orderInfo: OrderInfo = JSON.parseObject(x, classOf[OrderInfo])
              orderInfo.consignee = orderInfo.consignee.substring(0, 1) + "**" // 李小名 => 李**
              orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "****" + orderInfo.consignee_tel.substring(7, 11)
              // 计算 createDate 和 createHour
              val ts: String = orderInfo.create_time
              val date: Date = dateFormat.parse(ts)
              orderInfo.create_date = dateFormat1.format(date)
              orderInfo.create_hour = dateFormat2.format(date)
              orderInfo
            })
          })
        orderInfoRdd.foreach(println)

        //        输出到 本地 MySQL 中
        // 嫌麻烦,还要安装 Hadoop - HBase集群 和 phoenix
        //    所以使用 MySql 存储 日活数据
        import sparkSession.implicits._
        val orderInfoDs: Dataset[OrderInfo] = orderInfoRdd.toDS()
        val properties: Properties = new Properties()
        properties.setProperty("user", PropertiesUtil.getProperty("mysql.user"))
        properties.setProperty("password", PropertiesUtil.getProperty("mysql.password"))
        // 4.将数据写到Mysql中
        orderInfoDs.write.mode(SaveMode.Append).jdbc(PropertiesUtil.getProperty("mysql.url"), "gmall_order_info", properties)

        //        /* Phoenix 建表语句
        //        create table gmall_order_info (
        //           id varchar primary key,
        //           province_id varchar,
        //           consignee varchar,
        //           order_comment varchar,
        //           consignee_tel varchar,
        //           order_status varchar,
        //           payment_way varchar,
        //           user_id varchar,
        //           img_url varchar,
        //           total_amount decimal,
        //           expire_time varchar,
        //           delivery_address varchar,
        //           create_time varchar,
        //           operate_time varchar,
        //           tracking_no varchar,
        //           parent_order_id varchar,
        //           out_trade_no varchar,
        //           trade_body varchar,
        //           create_date varchar,
        //           create_hour varchar)
        //         */
        //        import org.apache.phoenix.spark._
        //        orderInfoRdd.saveToPhoenix(
        //          "GMALL_ORDER_INFO",
        //          Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL",
        //            "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        //          zkUrl = Some("hadoop201,hadoop202,hadoop203:2181"))

        for (o <- offsetRanges) {
          val zkPath: String = s"${zkTopicPath}/${o.partition}"
          ZkUtils(zkClient, false).updatePersistentPath(zkPath, o.untilOffset.toString)
        }

      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
