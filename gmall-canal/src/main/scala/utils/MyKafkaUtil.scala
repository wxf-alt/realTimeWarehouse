package utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @Auther: wxf
 * @Date: 2023/1/28 16:35:27
 * @Description: MyKafkaUtil
 * @Version 1.0.0
 */
object MyKafkaUtil {

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092");
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  def send(topic: String, content: String): Unit = {
    val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, content)
    producer.send(record)
  }

}
