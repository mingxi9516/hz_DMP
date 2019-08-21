package tags

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import scalaUtils.TagsUtils

/**
  * 创建上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()

    //加载字典文件
    val dirLines = spark.sparkContext.textFile("F:\\DMP\\app_dict.txt")
    //过滤字典文件
    val dirFile: collection.Map[String, String] = dirLines.map(data => data.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collectAsMap()

    //将字典文件广播出去
    val dirBroadcast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(dirFile)

    //加载停用词库
    val wordsLines = spark.sparkContext.textFile("F:\\DMP\\stopwords.txt")
    //广播停用词库
    val keyWordBroadcast: Broadcast[Array[String]] = spark.sparkContext.broadcast(wordsLines.collect())

    //加载hbase配置文件
    val config = ConfigFactory.load()
    //获取表名
    val hbaseTableName = config.getString("hbase.table.Name")
    val zkHost = config.getString("hbase.zookeeper.host")
    //创建hadoop任务配置
    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",zkHost)
    //创建hbase连接
    val connection = ConnectionFactory.createConnection(configuration)
    val admin = connection.getAdmin
    if(!admin.tableExists(TableName.valueOf(hbaseTableName))){
      println("表可用")
      //创建表的对象
      val hbaseTableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      //创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      //将列簇加载到表中
      hbaseTableDescriptor.addFamily(columnDescriptor)
      //将创建好的表进行加载
      admin.createTable(hbaseTableDescriptor)
      //关闭
      admin.close()
      connection.close()
    }else{
      println("表已经存在")
    }
    //加载hbase相关属性配置
    val jobConf = new JobConf(configuration)
    //指定key的输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出到那张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)


    //读取parquet文件
    val dataFrame = spark.read.parquet("F:\\DMP\\output0\\")
    //过滤parquet文件数据
    val unit = dataFrame.filter(TagsUtils.userIdOne)
      // 根据每一条数据， 打对应的标签 （7个标签）
      .rdd.map(data => {
      //获取用户id
      val userId = TagsUtils.getAllOneUserId(data)

      //广告位类型标签和渠道标签
      val adTags: List[(String, Int)] = TagsAD.getTatgs(data)

      //App 名称标签
      val appTags = TagsApp.getTatgs(data,dirBroadcast)

      //设备标签
      val equipmentTags = TagsEquipment.getTatgs(data)

      //关键字标签
      val keyWordsTags: List[(String, Int)] = TagsKeyWords.getTatgs(data, keyWordBroadcast)

      //地域标签
      val regionalTags = TagsRegional.getTatgs(data)

      //商业圈标签
      val businessTags = TagsBusiness.getTatgs(data)
      (userId,adTags++appTags++equipmentTags++keyWordsTags++regionalTags++businessTags)
    }).reduceByKey((list1,list2)=>{
      (list1:::list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft(0)(_+_._2))
        .toList
    }).map{
      case (userId,userTags)=>{
        val put = new Put(Bytes.toBytes(userId))
        val tags = userTags.map(t=>t._1+":"+t._2).mkString(",")
        //对应的rowKey下面的列，列名，列的value值
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("20190725"),Bytes.toBytes(tags))
        //存入hbase
        (new ImmutableBytesWritable(),put)
      }
    }
    //运行这个job任务
      .saveAsHadoopDataset(jobConf)
    spark.sparkContext.stop()




  }
}
