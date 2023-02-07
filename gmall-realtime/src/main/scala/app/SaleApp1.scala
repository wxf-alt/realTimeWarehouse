package app

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.alibaba.fastjson.JSON
import common.Constant
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis
import utils.{MyKafkaUtil, PropertiesUtil, RedisUtil}

/**
 * @Auther: wxf
 * @Date: 2023/2/3 14:56:22
 * @Description: SaleApp
 * @Version 1.0.0
 */
object SaleApp1 {

  // 输出Redis
  def saveRedis(jedis: Jedis, key: String, value: AnyRef): Unit = {
    // 方式一：
    //    implicit val f: DefaultFormats.type = org.json4s.DefaultFormats
    //    Serialization.write(value)
    import org.json4s.DefaultFormats
    val jsonValue: String = Serialization.write(value)(DefaultFormats)
    //    jedis.set(key, jsonValue)
    // 设置过期时间 10秒
    jedis.set(key, jsonValue, "NX", "EX", 60 * 30L)
    //    jedis.setex(key,10,jsonValue)
  }

  // 缓存订单信息
  def cacheOrderInfo(jedis: Jedis, orderInfo: OrderInfo): Unit = {
    val key: String = s"order_info:${orderInfo.id}"
    saveRedis(jedis, key, orderInfo)
  }

  // 缓存订单明细
  def cacheOrderDetail(jedis: Jedis, orderDetail: OrderDetail): Unit = {
    val key: String = s"order_detail:${orderDetail.order_id}:${orderDetail.id}"
    saveRedis(jedis, key, orderDetail)
  }

  import scala.collection.convert.wrapAll._

  def funllJoin(orderInfoStream: DStream[(String, OrderInfo)], orderDetailStream: DStream[(String, OrderDetail)]): DStream[SaleDetail] = {
    orderInfoStream.fullOuterJoin(orderDetailStream)
      .mapPartitions(it => {
        // 创建 Redis 连接 客户端
        val jedis: Jedis = RedisUtil.getClient()
        // 业务逻辑处理   1 可以 和 3 写在一起
        val result: Iterator[SaleDetail] = it.flatMap {
          // 1.orderInfo和orderDetail存在匹配数据
          case (orderId, (Some(orderInfo), opt)) =>
            // 1.1 将 orderInfo 数据写到Redis缓存  类型：String
            cacheOrderInfo(jedis, orderInfo)
            // 1.3 读取Redis查看是否有匹配数据
            val keys: List[String] = jedis.keys(s"order_detail:${orderId}:*").toList
            // 1.3.1 在Redis中获取到对应信息
            keys.map(x => {
              val orderDetail: OrderDetail = JSON.parseObject(jedis.get(x), classOf[OrderDetail])
              // 删除 key 防止重复匹配数据
              println(s"删除 x1 --》 key:${x}")
              jedis.del(x)
              SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
            }) :::
              (opt match {
                case Some(orderDetail) => SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
                case None => Nil
              })
          //          case (orderId, (Some(orderInfo), Some(orderDetail))) =>
          //            println("进入 falg_1")
          //            // 1.1 将 orderInfo 数据写到Redis缓存  类型：String
          //            cacheOrderInfo(jedis, orderInfo)
          //            // 1.2 封装输出样例类
          //            val saleDetail: SaleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          //            // 1.3 读取Redis查看是否有匹配数据
          //            val keys: List[String] = jedis.keys(s"order_detail:${orderId}:*").toList
          //            // 1.3.1 在Redis中获取到对应信息
          //            saleDetail :: keys.map(x => {
          //              val orderDetail: OrderDetail = JSON.parseObject(jedis.get(x), classOf[OrderDetail])
          //              // 删除 key 防止重复匹配数据
          //              println(s"x1 --》 key:${x}")
          //              jedis.del(x)
          //              SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          //            })
          //          // 3.orderIdfo存在，但是orderDetail没有对应的数据
          //          case (orderId, (Some(orderInfo), None)) =>
          //            println("进入 falg_2")
          //            // 3.1 orderInfo 数据需要写入缓存 可能出现延迟数据
          //            cacheOrderInfo(jedis, orderInfo)
          //            // 3.2 根据orderId去缓存中读取对应的 多个 OrderDetail信息(集合)
          //            //            val keys: List[String] = jedis.keys(s"order_detail:${orderInfo.id}:*").toList
          //            val keys: List[String] = jedis.keys(s"order_detail:${orderId}:*").toList
          //            // 3.2.1 在Redis中获取到对应信息
          //            keys.map(x => {
          //              val orderDetail: OrderDetail = JSON.parseObject(jedis.get(x), classOf[OrderDetail])
          //              // 删除 key 防止重复匹配数据
          //              println(s"x2 --》 key:${x}")
          //              jedis.del(x)
          //              SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          //            })
          case (orderId, (None, Some(orderDetail))) => // 2.orderIdfo没有对应的数据，但是orderDetail存在
            // 2.1 根据orderDetail中的orderId去 Redis中获取对应的OrderInfo信息
            val orderInfo: String = jedis.get("order_info:" + orderId)
            if (null != orderInfo && orderInfo.nonEmpty) {
              // 2.1.1 可以获取对应的匹配信息  匹配输出样例类
              val info: OrderInfo = JSON.parseObject(orderInfo, classOf[OrderInfo])
              SaleDetail().mergeOrderInfo(info).mergeOrderDetail(orderDetail) :: Nil
            } else {
              // 2.1.2 没有获取到对应的orderInfo信息  需要将orderDetail信息缓存到Redis中
              cacheOrderDetail(jedis, orderDetail)
              Nil
            }

        }
        // 关闭 Redis 客户端连接
        jedis.close()
        // 返回结果
        result
      })
  }

  // 关联 User用户表 获取相关信息
  def joinUser(saleDetailStream: DStream[SaleDetail], ssc: StreamingContext): DStream[SaleDetail] = {
    // 1.每间隔3秒读取一次 MySql用户表
    saleDetailStream.transform(saleDetailRdd => {
      // 2.读取 MySql数据
      // 2.1 直接在 Driver 中将数据获取
      // 2.2 每个分区读取一次
      val url: String = PropertiesUtil.getProperty("mysql.s1.url")
      val props: Properties = new Properties()
      props.setProperty("user", PropertiesUtil.getProperty("mysql.user"))
      props.setProperty("password", PropertiesUtil.getProperty("mysql.password"))
      // 3.构建 SparkSession 在Driver中获取数据
      val sparkSession: SparkSession = SparkSession.builder()
        .config(ssc.sparkContext.getConf)
        .getOrCreate()
      import sparkSession.implicits._
      val userInfoDF: RDD[(String, UserInfo)] = sparkSession
        .read.jdbc(url, "user_info", props)
        .as[UserInfo]
        .rdd
        .map(user => (user.id, user))
      // 4.join 连接 获取 user相关数据
      saleDetailRdd.map(x => (x.user_id, x))
        .join(userInfoDF)
        .map {
          case (_, (saleDetail, userInfo)) =>
            saleDetail.mergeUserInfo(userInfo)
        }
    })

  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    conf.set("es.nodes", PropertiesUtil.getProperty("es.nodes"))
    conf.set("es.port", PropertiesUtil.getProperty("es.port"))
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))

    val (kafkaOrderInfoStream, zkClient1, zkTopicPath1): (InputDStream[ConsumerRecord[String, String]], ZkClient, String) =
      MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_ORDER_INFO, PropertiesUtil.getProperty("kafka.sale.orderInfo.group.id"))
    val (kafkaOrderDetailStream, zkClient2, zkTopicPath2): (InputDStream[ConsumerRecord[String, String]], ZkClient, String) =
      MyKafkaUtil.getKafkaStream(ssc, Constant.TOPIC_ORDER_DETAIL, PropertiesUtil.getProperty("kafka.sale.orderDetail.group.id"))

    //通过rdd转换得到偏移量的范围
    var infoOffsetRanges: Array[OffsetRange] = Array[OffsetRange]()
    var detailOffsetRanges: Array[OffsetRange] = Array[OffsetRange]()

    // 订单流
    val orderInfoStream: DStream[(String, OrderInfo)] = kafkaOrderInfoStream.transform(rdd => {
      infoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
      .mapPartitions(it => {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateFormat1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val dateFormat2: SimpleDateFormat = new SimpleDateFormat("HH")
        it.map(x => {
          val orderInfo: OrderInfo = JSON.parseObject(x.value(), classOf[OrderInfo])
          orderInfo.consignee = orderInfo.consignee.substring(0, 1) + "**" // 李小名 => 李**
          orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "****" + orderInfo.consignee_tel.substring(7, 11)
          // 计算 createDate 和 createHour
          val ts: String = orderInfo.create_time
          val date: Date = dateFormat.parse(ts)
          orderInfo.create_date = dateFormat1.format(date)
          orderInfo.create_hour = dateFormat2.format(date)
          (orderInfo.id, orderInfo)
        })
      })

    // 订单明细流
    val orderDetailStream: DStream[(String, OrderDetail)] = kafkaOrderDetailStream.transform(rdd => {
      detailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
      .map(x => {
        val orderDetail: OrderDetail = JSON.parseObject(x.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })

    var saleDetailStream: DStream[SaleDetail] = funllJoin(orderInfoStream, orderDetailStream)

    saleDetailStream = joinUser(saleDetailStream, ssc)

    saleDetailStream.foreachRDD { saleRdd =>
      println("----------------------" + System.currentTimeMillis() + "----------------------")
      saleRdd.foreach(println)

      // 输出 ES
      import org.elasticsearch.spark._
      saleRdd.saveToEs(PropertiesUtil.getProperty("es.sale"))

      // 更新偏移量
      kafkaOrderInfoStream.asInstanceOf[CanCommitOffsets].commitAsync(infoOffsetRanges)
      kafkaOrderDetailStream.asInstanceOf[CanCommitOffsets].commitAsync(detailOffsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}