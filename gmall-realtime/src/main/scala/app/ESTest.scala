package app

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @Auther: wxf
 * @Date: 2023/2/2 11:35:05
 * @Description: ESTest
 * @Version 1.0.0
 */
object ESTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ESTest")
    conf.set("es.nodes", "nn1.hadoop")
    conf.set("es.port", "9200")

    // import org.elasticsearch.spark._
    //    // 读取数据
    //    val sc: SparkContext = new SparkContext(conf)
    //    val value: RDD[(String, collection.Map[String, AnyRef])] = sc.esRDD("user/_doc")
    //    val result = value.map({
    //      case (str, map) => map.toString
    //    })
    //    result.foreach(println)

    import org.elasticsearch.spark.streaming._
    // 写入数据
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6666)
    val recordStream: DStream[U1] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      U1(str(0), str(1).toInt)
    })
    // 使用 name字段 当作 id 写入ES
    recordStream.saveToEs("u1/_doc", Map("es.mapping.id" -> "name"))

    ssc.start()
    ssc.awaitTermination()

  }

  case class U1(name: String, age: Int)

}