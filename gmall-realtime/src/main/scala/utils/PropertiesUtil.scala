package utils

import java.io.InputStream
import java.util.Properties

/**
 * @Auther: wxf
 * @Date: 2023/1/13 15:24:25
 * @Description: PropertiesUtil
 * @Version 1.0.0
 */
class PropertiesUtil {}

object PropertiesUtil {

  private val inputStream: InputStream = classOf[PropertiesUtil].getClassLoader.getResourceAsStream("config.properties")
  //  val inputStream: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
  val properties: Properties = new Properties()
  properties.load(inputStream)

  def getProperty(propertyName: String): String = properties.getProperty(propertyName)

  //  // 测试读取 配置文件
  //  def main(args: Array[String]): Unit = {
  //    println(getProperty("kafka.broker.servers"))
  //  }

}
