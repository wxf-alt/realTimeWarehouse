import java.net.InetSocketAddress
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.google.protobuf.ByteString
import common.Constant
import utils.MyKafkaUtil

import scala.collection.convert.wrapAll._

/**
 * @Auther: wxf
 * @Date: 2023/1/28 11:24:59
 * @Description: CanalClient     ① Canal 解析 binlog 数据；② 封装成 Json；③ 发送至 Kafka
 * @Version 1.0.0
 */
object CanalClient {

  def main(args: Array[String]): Unit = {

    // 1.创建能连接到 Canal 的连接器对象
    val address: InetSocketAddress = new InetSocketAddress("s1.hadoop", 11111)
    val canalConnector: CanalConnector = CanalConnectors.newSingleConnector(address, "example", "", "")
    // 1.1 连接 canal
    canalConnector.connect()
    // 1.2 订阅数据  订阅 gmall数据库下所有表
    canalConnector.subscribe("gmall.*")


    // 2.读取数据，进行解析
    // 2.1 使用循环方式 持续从 canal服务中读取数据
    while (true) {
      // 2.2 一次从 canal 拉取最多100条sql数据引起的变化
      val message: Message = canalConnector.get(100)
      // 2.3 获取 Entry
      val entriesOption: Option[util.List[CanalEntry.Entry]] = if (message != null) Some(message.getEntries) else None
      if (entriesOption.isDefined && entriesOption.get.nonEmpty) {
        val entries: util.List[CanalEntry.Entry] = entriesOption.get
        //        println("entries --》" + entries.isEmpty)
        for (entry <- entries) {
          // 保证 EntryType 是 RowData 类型
          if (entry != null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA) {
            // 2.4 从 entry 中 获取 StoreValue
            val storeValue: ByteString = entry.getStoreValue
            // 2.5 解析 StoreValue 获取 RowChange
            val rowChange: RowChange = RowChange.parseFrom(storeValue)
            // 2.6 一个 RowChange 中有多个 RowData，每个 RowData 表示一行数据的变化
            val rowDatas: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
            // 解析 RowDatas 中每行每列的数据
            // 3.将数据 转成 JSON，写入到 kafka 中
            handleData(entry.getHeader.getTableName, rowDatas, rowChange.getEventType)
          }
        }
      }
      else {
        println("没有获取到数据，2s之后重试")
        Thread.sleep(2000)
      }
    }

  }

  // 处理 rowData 数据的工具类
  def handleData(tableName: String,
                 rowDatas: util.List[CanalEntry.RowData],
                 eventType: CanalEntry.EventType) = {
    if (tableName.toLowerCase == "order_info" &&
      eventType == EventType.INSERT &&
      rowDatas != null && rowDatas.nonEmpty) {
      for (rowData <- rowDatas) {
        val jsonObject: JSONObject = new JSONObject()
        // 1.获取 一行中 所有的 变化后 的列
        val columnList: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
        // 2.将多列数据封装到一个Json字符串中
        for (column <- columnList) {
          val name: String = column.getName
          val value: String = column.getValue
          jsonObject.put(name, value)
        }
        MyKafkaUtil.send(Constant.TOPIC_ORDER_INFO, jsonObject.toJSONString)
      }
    }
  }


}
