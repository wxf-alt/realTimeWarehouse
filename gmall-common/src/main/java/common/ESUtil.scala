package common

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}

/**
 * @Auther: wxf
 * @Date: 2023/1/31 19:57:50
 * @Description: ESUtil
 * @Version 1.0.0
 */
object ESUtil {

  private val esUrl: String = "http://nn1.hadoop:9200"

  // 配置信息
  val config: HttpClientConfig = new HttpClientConfig.Builder(esUrl)
    .maxTotalConnection(100) // 最多同时可以有100个到es的连接客户端
    .connTimeout(10 * 1000) // 连接到es的超时时间
    .readTimeout(10 * 1000) // 读取数据的超时时间
    .multiThreaded(true) // 是否允许多线程
    .build()
  // 1.1 创建一个客户端工厂(相当于连接池)
  val factory: JestClientFactory = new JestClientFactory
  factory.setHttpClientConfig(config)

  def main(args: Array[String]): Unit = {
    //    // 测试 单条写入
    //    val data: String =
    //      """
    //        |{
    //        |  "name":"张三",
    //        |  "age":25
    //        |}
    //        |""".stripMargin
    //    insertSingle("user", data)

    //    insetBulk

    // 测试 多条写入 自定义id
    val userList: List[(String, User)] = ("100", User("浮浮", 10)) :: ("101", User("安利", 11)) :: Nil
    insertBulk("user", userList.toIterator)

    //    // 测试 多条写入  随机id
    //    val userList: List[User] = User("浮浮", 10) :: User("安利", 11) :: Nil
    //    insertBulk("user", userList.toIterator)

  }


  /**
   * @Description:
   * @Author: wxf
   * @Date: 2023/1/31 20:37
   * @param index  index
   * @param source 数据源
   * @param id     单条数据的id，如果是null，会随机生成
   * @return: void
   **/
  def insertSingle(index: String, source: Object, id: String = null): Unit = {
    // 向es写入数据
    // 获取ES 客户端
    val client: JestClient = factory.getObject

    // 写入(单次，批次)
    val indexAction: Index = new Index.Builder(source)
      .index(index)
      .`type`("_doc")
      .id(id)
      .build()
    client.execute(indexAction)

    // 关闭客户端连接  将客户端还给工厂
    client.shutdownClient()
  }

  /**
   * @Description:
   * @Author: wxf
   * @Date: 2023/1/31 20:55
   * @param index   index
   * @param sources 数据源
   * @param id
   * @return: void
   **/
  def insertBulk(index: String, sources: Iterator[Any], id: String = null): Unit = {

    val client: JestClient = factory.getObject
    val bulkAction = new Bulk.Builder()
      .defaultIndex(index)
      .defaultType("_doc")

    sources.foreach {
      case (id: String, data) =>
        val index: Index = new Index.Builder(data).id(id).build()
        bulkAction.addAction(index)
      case data =>
        val index: Index = new Index.Builder(data).build()
        bulkAction.addAction(index)
    }

    client.execute(bulkAction.build())

    client.shutdownClient()
  }

  // 多条写入数据
  def insetBulk(): Unit = {
    val user1: User = User("小黄", 2)
    val user2: User = User("小白", 5)
    val index1: Index = new Index.Builder(user1).build()
    val index2: Index = new Index.Builder(user2).build()

    val client: JestClient = factory.getObject
    val bulkAction: Bulk = new Bulk.Builder()
      .defaultIndex("user")
      .defaultType("_doc")
      // 添加多条记录
      .addAction(index1)
      .addAction(index2)
      .build()
    client.execute(bulkAction)

    client.shutdownClient()
  }

}

case class User(name: String, age: Int)


//def main (args: Array[String] ): Unit = {
//
//  // 向es写入数据
//  // 1.ES 客户端
//  factory.setHttpClientConfig (config)
//  // 1.2 从工厂获取一个客户端
//  val client: JestClient = factory.getObject
//
//  // 2.ES 需要的数据(JSON，样例类)
//  /*val data: String =
//    """
//      |{
//      |  "name":"zhangsan",
//      |  "age":20
//      |}
//      |""".stripMargin*/
//  val data: user = user ("王五", 30)
//
//  // 3.写入(单次，批次)
//  val index: Index = new Index.Builder (data)
//  .index ("user")
//  .`type` ("_doc")
//  //      .id("3") // 可选，如果没有设置，id会自动生成
//  .build ()
//  client.execute (index)
//
//  // 4.关闭客户端连接  将客户端还给工厂
//  //    client.close()
//  client.shutdownClient ()
//
//  // 测试 单条写入
//  val data: String =
//  """
//    |{
//    |  "name":"张三",
//    |  "age":25
//    |}
//    |""".stripMargin
//  insertSingle ("user", data)
//}