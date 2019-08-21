package tags

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import scalaUtils.TagsUtils

import scala.collection.immutable

/**
  * 创建上下文标签
  */
object TagsContextV2 {
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
    val baseRDD = dataFrame.filter(TagsUtils.userIdOne).rdd.map(data => {
      //获取用户id
      val userId = TagsUtils.getAllUserId(data)
      (userId, data)
    })

    //构建点集合

    val verties: RDD[(Long, immutable.Seq[(Any, Int)])] = baseRDD.flatMap(data=>{
      val row: Row = data._2

      //广告位类型标签和渠道标签
      val adTags: List[(String, Int)] = TagsAD.getTatgs(row)

      //App 名称标签
      val appTags = TagsApp.getTatgs(row, dirBroadcast)

      //设备标签
      val equipmentTags = TagsEquipment.getTatgs(row)

      //关键字标签
      val keyWordsTags: List[(String, Int)] = TagsKeyWords.getTatgs(row, keyWordBroadcast)

      //地域标签
      val regionalTags = TagsRegional.getTatgs(row)

      //商业圈标签
      //val businessTags = TagsBusiness.getTatgs(data)
      val allTags = adTags ++ appTags ++ equipmentTags ++ keyWordsTags ++ regionalTags

      // 进行点集合构建
      val VD = data._1.map((_, 0)) ++ allTags
      // 注意两个问题 第一就是每一个UserId都是一个String类型，没法进行点与点之间的比较，所以需要我们转换类型
      // 在这里我用的是取HashCode值的方式进行转换类型，可以将String转换成我们需要的整数类型
      // 第二就是每一个点之中只能有个点携带标签，其他的点不需要携带标签，如果其他的点也携带了标签
      // 那么此时数据就会发生变化，造成重复现在，为了避免这种问题，我们就得保证一个点携带标签
      data._1.map(dt => {
        if (data._1.head.equals(dt)) {
          (dt.hashCode().toLong, VD)
        } else {
          (dt.hashCode().toLong,List.empty)
        }
      })
    })
    // 构建边的集合
    val edge: RDD[Edge[Int]] = baseRDD.flatMap(data => {
      data._1.map(dt =>{
        Edge(data._1.head.hashCode.toLong, dt.hashCode.toLong, 0)
      })
    })

    //构建图
    val graph: Graph[immutable.Seq[(Any, Int)], Int] = Graph(verties,edge)

    //// 取出顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //将顶点和我们的数据进行Join
    vertices.join(verties).map{
      case (userId,(comId,tagsAndUserId)) =>{
        (comId,tagsAndUserId)
      }
    }.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1)
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
