package utils

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.collection.mutable

/**
 * @Auther: wxf
 * @Date: 2023/1/13 15:16:51
 * @Description: MyKafkaUtil  Kafka工具类
 * @Version 1.0.0
 */
object MyKafkaUtil {

  //指定组名
  val group: String = PropertiesUtil.getProperty("kafka.group.id")
  //指定kafka的broker地址，SparkStream的Task直连到kafka的分区上，用底层的API消费，效率更高
  val brokerList: String = PropertiesUtil.getProperty("kafka.broker.servers")
  //指定zk的地址，更新消费的偏移量时使用，当然也可以使用Redis和MySQL来记录偏移量
  val zkQuorum: String = PropertiesUtil.getProperty("zkQuorum")

  //创建zk客户端，可以从zk中读取偏移量数据，并更新偏移量
  val zkClient: ZkClient = new ZkClient(zkQuorum)

  def getKafkaStream(ssc: StreamingContext, topic: String): (InputDStream[ConsumerRecord[String, String]], ZkClient, String) = {

    val topics: Set[String] = Set(topic)

    //topic在zk里的数据路径，用于保存偏移量
    val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(group, topic)
    //得到zk中的数据路径 例如："/consumers/${group}/offsets/${topic}"
    val zkTopicPath: String = s"${topicDirs.consumerOffsetDir}"
    //    println("zkTopicPath：" + zkTopicPath)

    //kafka参数
    val kafkaParams = Map(
      "bootstrap.servers" -> brokerList,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean),
      //earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      //none  topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      "auto.offset.reset" -> "latest"
    )

    //定义一个空的kafkaStream，之后根据是否有历史的偏移量进行选择
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null

    //如果存在历史的偏移量，那使用fromOffsets来存放存储在zk中的每个TopicPartition对应的offset
    var fromOffsets: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]

    //从zk中查询该数据路径下是否有每个partition的offset，这个offset是我们自己根据每个topic的不同partition生成的
    //数据路径例子：/consumers/${group}/offsets/${topic}/${partitionId}/${offset}"
    //zkTopicPath = /consumers/qingniu/offsets/hainiu_qingniu/
    val children: Int = zkClient.countChildren(zkTopicPath)

    //判断zk中是否保存过历史的offset
    if (children > 0) {
      for (i <- 0 until children) {
        // /consumers/qingniu/offsets/hainiu_qingniu/0
        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/${i}")
        // hainiu_qingniu/0
        val tp: TopicPartition = new TopicPartition(topic, i)
        //将每个partition对应的offset保存到fromOffsets中
        // hainiu_qingniu/0 -> 888
        fromOffsets += tp -> partitionOffset.toLong
      }
      //通过KafkaUtils创建直连的DStream，并使用fromOffsets中存储的历史偏离量来继续消费数据
      kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    } else {
      //如果zk中没有该topic的历史offset，那就根据kafkaParam的配置使用最新(latest)或者最旧的(earliest)的offset
      kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    }

    (kafkaStream, zkClient, zkTopicPath)

  }

}
