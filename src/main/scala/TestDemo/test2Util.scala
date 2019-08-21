package TestDemo

import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.Row
import tags.Tags

object test2Util {
  def getTags(data:JSONObject): List[(String,Int)] = {
    val buffer = collection.mutable.ListBuffer[String]()


    // 创建返回值集合
    var list = List[(String, Int)]()
    val status = data.getIntValue("status")
    if (status == 0) return null
    // 如果不为空 进行取值操作
    val regeocodeJson = data.getJSONObject("regeocode")
    if (regeocodeJson == null) return null
    val pois = regeocodeJson.getJSONArray("pois")
    if (pois == null) return null
    // 循环处理json内的数组
    for (i<-pois.toArray){
      if(i.isInstanceOf[JSONObject]){
        val json = i.asInstanceOf[JSONObject]
          if((json.getString("type").length>0)){
          var typeName1 = json.getString("type")
          val strings: Array[String] = typeName1.split(";")
          for(str <- strings){
            list:+=(str,1)
          }

        }
      }
    }
    list
  }
}
