package TestDemo

import com.alibaba.fastjson.JSONObject

object test1Util {
  def getCusinessareaCount(data:JSONObject): List[(String, Int)] ={
    // 创建返回值集合
    var list = List[(String,Int)]()
    val status = data.getIntValue("status")
    if(status==0) return null
    // 如果不为空 进行取值操作
    val regeocodeJson = data.getJSONObject("regeocode")
    if(regeocodeJson == null) return null
    val pois = regeocodeJson.getJSONArray("pois")
    if(pois == null) return null
    // 循环处理json内的数组
    for (i<-pois.toArray){
      if(i.isInstanceOf[JSONObject]){
        val json = i.asInstanceOf[JSONObject]
        if(!"[]".equalsIgnoreCase(json.getString("businessarea"))){
          list:+=(json.getString("businessarea"),1)
        }
      }
    }
    list
  }
}
