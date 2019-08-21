package spark

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}


object regionalCount {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    //读取parquet文件
    val dataFrame = spark.read.parquet("F:\\DMP\\output0\\")
    //创建表
    dataFrame.createTempView("parquetTable")
    val wideTable = spark.sql(
      "select " +
      "provincename," +
      "cityname," +
      "ispname, "+
      "networkmannername, "+
      "devicetype, "+
      "client, "+
      "adplatformproviderid, "+
      "case when requestmode=1 and processnode>=1 then 1 else 0 end original_request," +
      "case when requestmode=1 and processnode>=2 then 1 else 0 end valid_request," +
      "case when requestmode=1 and processnode=3 then 1 else 0 end ad_request," +
      "case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end bidding_count," +
      "case when iseffective=1 and isbilling=1 and isbid=1 and iswin=1 and adorderid != 0 then 1 else 0 end success_count," +
      "case when requestmode=2 and iseffective=1 then 1 else 0 end show_count," +
      "case when requestmode=3 and iseffective=1 then 1 else 0 end click_count," +
      "case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000.0 else 0 end ad_spending," +
      "case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000.0 else 0 end ad_costs " +
      "from " +
      "parquetTable"
    )
    wideTable.createTempView("wideTable")

    //加载MySQL配置
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))

    /**
      * 地域分布
      */
    val result1 = spark.sql("select " +
      "provincename, " +
      "cityname, " +
      "sum(original_request) original_request, " +
      "sum(valid_request) valid_request, " +
      "sum(ad_request) ad_request, " +
      "sum(bidding_count) bidding_count, " +
      "sum(success_count) success_count, " +
      "sum(show_count) show_count, " +
      "sum(click_count) click_count, " +
      "sum(ad_spending) ad_spending, " +
      "sum(ad_costs) ad_costs " +
      "from  " +
      "wideTable " +
      "group by  " +
      "provincename,cityname"
    )
    //result1.write.jdbc(load.getString("jdbc.url"),"regionalCount",prop)

    /**
      * 终端设备-运营商
      */
    val result2 = spark.sql("select " +
      "ispname, " +
      "sum(original_request) original_request, " +
      "sum(valid_request) valid_request, " +
      "sum(ad_request) ad_request, " +
      "sum(bidding_count) bidding_count, " +
      "sum(success_count) success_count, " +
      "sum(show_count) show_count, " +
      "sum(click_count) click_count, " +
      "sum(ad_spending) ad_spending, " +
      "sum(ad_costs) ad_costs " +
      "from  " +
      "wideTable " +
      "group by  " +
      "ispname"
    )
    //result2.write.jdbc(load.getString("jdbc.url"),"operation",prop)

    /**
      * 终端设备-网络
      */
    val result3 = spark.sql("select " +
      "networkmannername, " +
      "sum(original_request) original_request, " +
      "sum(valid_request) valid_request, " +
      "sum(ad_request) ad_request, " +
      "sum(bidding_count) bidding_count, " +
      "sum(success_count) success_count, " +
      "sum(show_count) show_count, " +
      "sum(click_count) click_count, " +
      "sum(ad_spending) ad_spending, " +
      "sum(ad_costs) ad_costs " +
      "from  " +
      "wideTable " +
      "group by  " +
      "networkmannername"
    )
    //result3.write.jdbc(load.getString("jdbc.url"),"network",prop)

    /**
      * 终端设备-设备
      */
    val result4 = spark.sql("select " +
      "devicetype, " +
      "sum(original_request) original_request, " +
      "sum(valid_request) valid_request, " +
      "sum(ad_request) ad_request, " +
      "sum(bidding_count) bidding_count, " +
      "sum(success_count) success_count, " +
      "sum(show_count) show_count, " +
      "sum(click_count) click_count, " +
      "sum(ad_spending) ad_spending, " +
      "sum(ad_costs) ad_costs " +
      "from  " +
      "wideTable " +
      "group by  " +
      "devicetype"
    )
    //result4.write.jdbc(load.getString("jdbc.url"),"equipment",prop)


    /**
      * 终端设备-操作系统
      */
    val result5 = spark.sql("select " +
      "client, " +
      "sum(original_request) original_request, " +
      "sum(valid_request) valid_request, " +
      "sum(ad_request) ad_request, " +
      "sum(bidding_count) bidding_count, " +
      "sum(success_count) success_count, " +
      "sum(show_count) show_count, " +
      "sum(click_count) click_count, " +
      "sum(ad_spending) ad_spending, " +
      "sum(ad_costs) ad_costs " +
      "from  " +
      "wideTable " +
      "group by  " +
      "client"
    )
    //result5.write.jdbc(load.getString("jdbc.url"),"os",prop)

    /**
      * 终端设备-渠道报表
      */
    val result6 = spark.sql("select " +
      "adplatformproviderid, " +
      "sum(original_request) original_request, " +
      "sum(valid_request) valid_request, " +
      "sum(ad_request) ad_request, " +
      "sum(bidding_count) bidding_count, " +
      "sum(success_count) success_count, " +
      "sum(show_count) show_count, " +
      "sum(click_count) click_count, " +
      "sum(ad_spending) ad_spending, " +
      "sum(ad_costs) ad_costs " +
      "from  " +
      "wideTable " +
      "group by  " +
      "adplatformproviderid"
    )
    result6.write.jdbc(load.getString("jdbc.url"),"adplatformprovider",prop)

  }
}
