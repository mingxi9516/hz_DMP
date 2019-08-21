package parquets

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scalaUtils.StringUtil

object BZ2Parquet {
  def main(args: Array[String]): Unit = {
    // 首先判断一下目录参数是否为空
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    // 创建数组存储输入输出目录
    val Array(input,output) = args

    //创建sparksession
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]")
      // 设置序列化机制
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //设置Spark sql 压缩方式，注意spark1.6版本默认不是snappy，到2.0以后是默认的压缩方式
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //读取文件
    val lines = spark.sparkContext.textFile(input)
    //过滤
    val filterLines: RDD[Array[String]] = lines.map(data=>data.split(",",data.length)).filter(data=>data.length>=85)
    val qf = filterLines.map(arr => {
      QF(
        arr(0),
        StringUtil.StringtoInt(arr(1)),
        StringUtil.StringtoInt(arr(2)),
        StringUtil.StringtoInt(arr(3)),
        StringUtil.StringtoInt(arr(4)),
        arr(5),
        arr(6),
        StringUtil.StringtoInt(arr(7)),
        StringUtil.StringtoInt(arr(8)),
        StringUtil.StringtoDouble(arr(9)),
        StringUtil.StringtoDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StringUtil.StringtoInt(arr(17)),
        arr(18),
        arr(19),
        StringUtil.StringtoInt(arr(20)),
        StringUtil.StringtoInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StringUtil.StringtoInt(arr(26)),
        arr(27),
        StringUtil.StringtoInt(arr(28)),
        arr(29),
        StringUtil.StringtoInt(arr(30)),
        StringUtil.StringtoInt(arr(31)),
        StringUtil.StringtoInt(arr(32)),
        arr(33),
        StringUtil.StringtoInt(arr(34)),
        StringUtil.StringtoInt(arr(35)),
        StringUtil.StringtoInt(arr(36)),
        arr(37),
        StringUtil.StringtoInt(arr(38)),
        StringUtil.StringtoInt(arr(39)),
        StringUtil.StringtoDouble(arr(40)),
        StringUtil.StringtoDouble(arr(41)),
        StringUtil.StringtoInt(arr(42)),
        arr(43),
        StringUtil.StringtoDouble(arr(44)),
        StringUtil.StringtoDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StringUtil.StringtoInt(arr(57)),
        StringUtil.StringtoDouble(arr(58)),
        StringUtil.StringtoInt(arr(59)),
        StringUtil.StringtoInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StringUtil.StringtoInt(arr(73)),
        StringUtil.StringtoDouble(arr(74)),
        StringUtil.StringtoDouble(arr(75)),
        StringUtil.StringtoDouble(arr(76)),
        StringUtil.StringtoDouble(arr(77)),
        StringUtil.StringtoDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StringUtil.StringtoInt(arr(84))
      )
    })
    val dataFrame = spark.createDataFrame(qf)
    //输出文件
    //dataFrame.write.parquet(output)

  }
}

case class  QF(
                sessionid: String,
                advertisersid: Int,
                adorderid: Int,
                adcreativeid: Int,
                adplatformproviderid: Int,
                sdkversion: String,
                adplatformkey: String,
                putinmodeltype: Int,
                requestmode: Int,
                adprice: Double,
                adppprice: Double,
                requestdate: String,
                ip: String,
                appid: String,
                appname: String,
                uuid: String,
                device: String,
                client: Int,
                osversion: String,
                density: String,
                pw: Int,
                ph: Int,
                lon: String,
                lat: String,
                provincename: String,
                cityname: String,
                ispid: Int,
                ispname: String,
                networkmannerid: Int,
                networkmannername:
                String,
                iseffective: Int,
                isbilling: Int,
                adspacetype: Int,
                adspacetypename: String,
                devicetype: Int,
                processnode: Int,
                apptype: Int,
                district: String,
                paymode: Int,
                isbid: Int,
                bidprice: Double,
                winprice: Double,
                iswin: Int,
                cur: String,
                rate: Double,
                cnywinprice: Double,
                imei: String,
                mac: String,
                idfa: String,
                openudid: String,
                androidid: String,
                rtbprovince: String,
                rtbcity: String,
                rtbdistrict: String,
                rtbstreet: String,
                storeurl: String,
                realip: String,
                isqualityapp: Int,
                bidfloor: Double,
                aw: Int,
                ah: Int,
                imeimd5: String,
                macmd5: String,
                idfamd5: String,
                openudidmd5: String,
                androididmd5: String,
                imeisha1: String,
                macsha1: String,
                idfasha1: String,
                openudidsha1: String,
                androididsha1: String,
                uuidunknow: String,
                userid: String,
                iptype: Int,
                initbidprice: Double,
                adpayment: Double,
                agentrate: Double,
                lomarkrate: Double,
                adxrate: Double,
                title: String,
                keywords: String,
                tagid: String,
                callbackdate: String,
                channelid: String,
                mediatype: Int

              )
