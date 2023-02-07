package com.example.gmallpublisher.service

/**
 * @Auther: wxf
 * @Date: 2023/2/7 16:05:32
 * @Description: DSLs
 * @Version 1.0.0
 */
object DSLs {

  def getSaleDetailDSL(date: String,
                      keyWord: String,
                      startPage: Int,
                      sizePerPage: Int,
                      aggField: String,
                      aggCount: Int) = {
    // "operator": "or"  默认使用 or 连接 代表 （包含 小米 或者 手机）
    // 设置为 and （代表 既包含 小米 又包含 手机）
    s"""
       |{
       |  "query": {
       |    "bool": {
       |      "filter": {
       |        "term": {
       |          "dt": "${date}"
       |        }
       |      },
       |      "must": [
       |        {"match": {
       |          "sku_name": {
       |            "query": "${keyWord}",
       |            "operator": "or"
       |          }
       |        }}
       |      ]
       |    }
       |  },
       |  "aggs": {
       |    "group_${aggField}": {
       |      "terms": {
       |        "field": "${aggField}",
       |        "size": ${aggCount}
       |      }
       |    }
       |  },
       |  "from": ${(startPage - 1) * sizePerPage},
       |  "size": ${sizePerPage}
       |}
       |""".stripMargin
  }

}
