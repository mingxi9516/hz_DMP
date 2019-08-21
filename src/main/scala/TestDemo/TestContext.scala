package TestDemo

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}

object TestContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //读取json文件
    val lines = sc.textFile("E:\\千锋大数据\\课堂资料\\项目三 （用户画像）\\项目（三）05\\json.txt")
    //解析
    lines.map(line=>{
      val jsonObject: JSONObject = JSON.parseObject(line)
      test1Util.getCusinessareaCount(jsonObject)
    }).map(t=>{
      t.groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
    }).filter(_.length!=0).foreach(println)


    /**
      * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
      */
    lines.map(line=>{
      val jsonObject: JSONObject = JSON.parseObject(line)
      test2Util.getTags(jsonObject)
    }).map(t=>{
      t.groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2))
    }).foreach(println)
  }
}
