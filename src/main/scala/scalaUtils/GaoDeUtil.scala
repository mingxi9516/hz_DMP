package scalaUtils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import scala.collection.mutable.ListBuffer


object GaoDeUtil {
  def getBuseness(long:Double,lat:Double):String={
   //https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=84de12c28ab11a4dab37577e570bee7a&radius=1000&extensions=all
    val location = long+","+lat
    //拼接url
    val url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location="+location+"&key=84de12c28ab11a4dab37577e570bee7a&radius=1000&extensions=all"
    // 调用地图 发送http请求
    val json = HttpUtil.get(url)
    //解析json字符串
    val jSONObject = JSON.parseObject(json)
    //判断请求状态
    val status = jSONObject.get("status")
    //如果等于0返回null，否则进行取值操作
    if(status == 0) return null
    val regeocode: JSONObject = jSONObject.getJSONObject("regeocode")
    if(regeocode == null ) return null
    val addressComponent: JSONObject = regeocode.getJSONObject("addressComponent")
    if(addressComponent == null) return null
    val businessAreas: JSONArray = addressComponent.getJSONArray("businessAreas")
    if(businessAreas == null) return null
    //创建返回值集合
    val buffer = ListBuffer[String]()

    for(array <- businessAreas.toArray) {
      if(array.isInstanceOf[JSONObject]){
        val arrayJsonObject = array.asInstanceOf[JSONObject]
        val name = arrayJsonObject.getString("name")
        if(name == null) return null
        buffer.append(name)
      }
    }

    buffer.mkString(",")
  }
}
