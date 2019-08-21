package spark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scalaUtils.AppUtils

object mediaAnalyze {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val dataFrame = spark.read.parquet("F:\\DMP\\output0\\")
    //获取parquet文件

    //读取字典文件
    val lines = spark.sparkContext.textFile("F:\\DMP\\app_dict.txt")
    //过滤
    val map = lines.map(data=>data.split("\t",-1)).filter(_.length>=5).map(arr=>(arr(4),arr(1))).collect().toMap
    //将字典文件广播出去
    val broadcast = spark.sparkContext.broadcast(map)

    val result: RDD[(String, List[Double])] = dataFrame.rdd.map(row => {
      // 通过广播变量进行判断取值
      var appname = row.getAs[String]("appname")
      if (!StringUtils.isNotBlank(appname)) {
        appname = broadcast.value.getOrElse(row.getAs[String]("appid"), "unknow")
      }
      // 先去处理原始、有效、广告请求
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      // 参与竞价数 竞价成功数 广告成品 广告消费
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 调用业务方法
      val reqList = AppUtils.Request(requestmode, processnode)
      val adList = AppUtils.appad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      val clickList = AppUtils.ReCount(requestmode, iseffective)
      (appname, reqList ++ adList ++ clickList)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })

    //保存本地磁盘
    result.map(data=>{
      (data._1+","+data._2.mkString(","))
    }).coalesce(10).saveAsTextFile("F:\\DMP\\mediaAnalyze")






  }
}
