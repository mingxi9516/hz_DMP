package tags

import org.apache.spark.sql.SparkSession

object busenessTest {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    //读取parquet文件
    val dataFrame = spark.read.parquet("F:\\DMP\\output0\\")
    dataFrame.rdd.map(data=>{
      TagsBusiness.getTatgs(data)
    }).foreach(println)
  }
}
