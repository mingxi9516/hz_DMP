package tags

import org.apache.spark.sql.Row

object TagsEquipment extends Tags {
  /**
    * 设备标签
    */
  override def getTatgs(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //获取参数
    val data = args(0).asInstanceOf[Row]

    //设备操作系统
    val osNum = data.getAs[Int]("client")
    osNum match {
      case 1 => list:+=("Android D00010001",1)
      case 2 => list:+=("IOS D00010002",1)
      case 4 => list:+=("WinPhone D00010003",1)
      case _ => list:+=("其他D00010004",1)
    }

    //设 备 联 网 方 式
    val networkmannername = data.getAs[String]("networkmannername")
    networkmannername match {
      case "Wifi" => list:+=("WIFI D00020001",1)
      case "4G" => list:+=("4G D00020002",1)
      case "3G" => list:+=("3G D00020003",1)
      case "2G" => list:+=("2G D00020004",1)
      case _ => list:+=("D00020005",1)
    }

    //设备运营商方式
    val ispname = data.getAs[String]("ispname")
    ispname match {
      case "移动" => list:+=("移动D00030001",1)
      case "联通" => list:+=("联通D00030002",1)
      case "电信" => list:+=("电信D00030003",1)
      case _ => list:+=("D00030004",1)
    }
    list
  }
}
