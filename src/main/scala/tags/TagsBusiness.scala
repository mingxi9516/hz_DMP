package tags

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import scalaUtils.{GaoDeUtil, JedisConnectionPool, StringUtil}

object TagsBusiness extends Tags{
  /**
    * 商业圈标签
    */
  override def getTatgs(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val data = args(0).asInstanceOf[Row]
    //获取经纬度
    val long = StringUtil.StringtoDouble(data.getAs[String]("lon"))
    val lat = StringUtil.StringtoDouble(data.getAs[String]("lat"))
    //过滤不是中国的经纬度
    if(long >= 73 && long <= 135 && lat >=3 && lat <= 54){
      val buseness = getBuseness(long,lat)
      if(StringUtils.isNotBlank(buseness)){
        val strings = buseness.split(",")
        strings.foreach(str=>{
          list:+=(str,1)
        })
      }
    }
    list
  }

  // 获取商圈信息
  def getBuseness(long:Double,lat:Double):String ={
    // 获取Key
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
    // 第一步去查询数据库
    var business = redisQuery(geoHash)
    // 第二步 如果数据库没有商圈 请求高德
    if(business == null || business.length == 0){
      val business = GaoDeUtil.getBuseness(long,lat)
      // 每次获取到新的商圈信息后，将新的商圈信息存入Redis中，进行缓存
      if(business != null || business.length>0){
        redisInsertByBusiness(geoHash,business)
      }
    }
    business
  }

  //通过Redis数据库获取商圈
  def redisQuery(geohash:String):String={
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }
  // 插入数据库
  def redisInsertByBusiness(geohash:String,business:String): Unit ={
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
