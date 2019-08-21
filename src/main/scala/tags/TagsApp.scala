package tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsApp extends Tags {
  /**
    * app名称标签
    */
  override def getTatgs(args: Any*): List[(String, Int)] = {
    var list = List[(  String,Int)]()

    //获取参数
    val data = args(0).asInstanceOf[Row]
    val dirFile: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

    //获取appname
    val appName = data.getAs[String]("appname")
    //获取appid
    val appId = data.getAs[String]("appid")

    if(StringUtils.isNotBlank(appName)){
      list:+=("APP"+appName,1)
    }else if( StringUtils.isNotBlank(appId)){
      list:+=("APP"+dirFile.value.getOrElse(appId,appId),1)
    }
    list

  }
}
