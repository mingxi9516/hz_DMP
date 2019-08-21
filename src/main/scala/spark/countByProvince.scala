package spark

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object countByProvince {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()

    //读取parquet文件
    val dataFrame = spark.read.parquet("F:\\DMP\\output0\\")
    //创建临时表
    dataFrame.createTempView("parquetTable")
    val result1 = spark.sql("select provincename,cityname,count(*) as ct from parquetTable group by provincename,cityname")
    //分区存入本地磁盘
    result1.write.format("json").partitionBy("provincename","cityname").save("F:\\DMP\\output1")

    //存入MySQL
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    //result1.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/hz_dmp","countByProvince",prop)
  }
}
