package bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @Auther: wxf
 * @Date: 2023/1/16 10:32:04
 * @Description: StartUpLog
 * @Version 1.0.0
 */
case class StartUpLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long,
                      var logDate: String = "",
                      var logHour: String = ""
                     )

//{
//  val date: Date = new Date(ts)
//  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
//  logHour = new SimpleDateFormat("HH").format(date)
//}
