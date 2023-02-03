package common

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{DocumentResult, Get, Index, Search, SearchResult}

/**
 * @Auther: wxf
 * @Date: 2023/2/1 09:34:55
 * @Description: TestESConnect
 * @Version 1.0.0
 */
object TestESConnect {

  def main(args: Array[String]): Unit = {
    //    putEs()
    //    getEs()
    searchEs()

  }

  def putEs(): Unit = {
    // 获取 es 连接
    val httpClientConfig: HttpClientConfig = new HttpClientConfig.Builder("http://nn1.hadoop:9200")
      .maxTotalConnection(20)
      .connTimeout(10 * 1000)
      .readTimeout(5 * 1000)
      .multiThreaded(true)
      .build()
    val jestClientFactory: JestClientFactory = new JestClientFactory()
    jestClientFactory.setHttpClientConfig(httpClientConfig)
    // 获取 客户端
    val esClient: JestClient = jestClientFactory.getObject

    val localDateTime: String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss:SSS"))
    val esBean: EsBean = EsBean("蜗牛壳", 13, localDateTime)
    val indexBuild: Index = new Index.Builder(esBean) // 会自动创建 index
      .index("es_bean")
      .`type`("_doc")
      .id("2")
      .build()
    esClient.execute(indexBuild)

    //    val result: DocumentResult = esClient.execute(indexBuild)
    //    val string: String = result.getSourceAsString
    //    println(string)

    // 关闭连接
    esClient.close()
  }


  def getEs(): Unit = {
    // 获取 es 连接
    val httpClientConfig: HttpClientConfig = new HttpClientConfig.Builder("http://nn1.hadoop:9200")
      .maxTotalConnection(20)
      .connTimeout(10 * 1000)
      .readTimeout(5 * 1000)
      .multiThreaded(true)
      .build()
    val jestClientFactory: JestClientFactory = new JestClientFactory()
    jestClientFactory.setHttpClientConfig(httpClientConfig)
    // 获取 客户端
    val esClient: JestClient = jestClientFactory.getObject

    val getAction: Get = new Get.Builder("es_bean", "1")
      .`type`("_doc")
      .build()
    val result: DocumentResult = esClient.execute(getAction)
    val string: String = result.getSourceAsString
    println(string)

    // 关闭连接
    esClient.close()
  }


  def searchEs(): Unit = {
    // 获取 es 连接
    val httpClientConfig: HttpClientConfig = new HttpClientConfig.Builder("http://nn1.hadoop:9200")
      .maxTotalConnection(20)
      .connTimeout(10 * 1000)
      .readTimeout(5 * 1000)
      .multiThreaded(true)
      .build()
    val jestClientFactory: JestClientFactory = new JestClientFactory()
    jestClientFactory.setHttpClientConfig(httpClientConfig)
    // 获取 客户端
    val esClient: JestClient = jestClientFactory.getObject

    val query: String =
      """
        |{
        |  "query": {"match_all": {}}
        |}
        |""".stripMargin
    val searchAction: Search = new Search.Builder(query)
      .addIndex("es_bean")
      .addType("_doc")
      .build()

    //    val result: SearchResult = esClient.execute(searchAction)
    //    println(result.getSourceAsString)

    val result: SearchResult = esClient.execute(searchAction)
    val resultList: util.List[String] = result.getSourceAsStringList
    import scala.collection.convert.wrapAll._
    resultList.foreach(println)

    // 关闭连接
    esClient.close()
  }


}

case class EsBean(name: String, age: Int, time: String)
