package tags

import org.apache.spark.sql.Row

object TagsRegional extends Tags {
  /**
    * 地域
    */
  override def getTatgs(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取参数
    val data = args(0).asInstanceOf[Row]

    //获取省
    val province = data.getAs[String]("provincename")
    list:+=("ZP"+province,1)
    //获取市
    val city = data.getAs[String]("cityname")
    list:+=("ZC"+city,1)
    list
  }
}
