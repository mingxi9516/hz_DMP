package tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsAD extends Tags {
  /**
    * 广告位类型标签
    * 广告位类型名称标签
    */
  override def getTatgs(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取参数
    val data = args(0).asInstanceOf[Row]
    // 获取广告类型 为广告类型打上标签
    val adType = data.getAs[Int]("adspacetype")
    adType match {
      case v if v >9 => list:+=("LC"+v,1)
      case v if v > 0 && v <= 9 => list:+=("LC0"+v, 1)
    }

    //获取广告位类型名称，为广告位类型名称打标签
    val adTypeName = data.getAs[String]("adspacetypename")
    if(StringUtils.isNoneBlank(adTypeName)){
      list:+=("LN"+adTypeName,1)
    }

    // 渠道标签
    val channel = data.getAs[Int]("adplatformproviderid")
    list:+=("CN"+channel,1)
    list
  }
}
