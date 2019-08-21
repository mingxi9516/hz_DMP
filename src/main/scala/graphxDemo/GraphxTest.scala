package graphxDemo

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphxTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()

    //创建点集合
    val vertexRDD: RDD[(Long, (String, Int))] = spark.sparkContext.makeRDD(Seq(
      (1L, ("小明", 50)),
      (2L, ("小红", 40)),
      (6L, ("小丁", 27)),
      (9L, ("小苍", 35)),
      (133L, ("小刚", 30)),
      (138L, ("小赵", 23)),
      (16L, ("小刘", 30)),
      (44L, ("小李", 35)),
      (21L, ("小迪", 25)),
      (5L, ("小王", 29)),
      (7L, ("小陈", 23)),
      (158L, ("小张", 26))
    ))

    //创建边集合
    val edgeRdd: RDD[Edge[Int]] = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L,2),
      Edge(2L, 133L,3),
      Edge(6L, 133L,4),
      Edge(9L, 133L,5),
      Edge(6L, 138L,7),
      Edge(16L, 138L,9),
      Edge(44L, 138L,4),
      Edge(21L, 138L,6),
      Edge(5L, 158L,8),
      Edge(7L, 158L,5)
    ))

    //构件图
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD,edgeRdd)
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

   vertices.foreach(println)

  val joinvalue: RDD[(VertexId, (VertexId, (String, Int)))] = vertices.join(vertexRDD)
    joinvalue.foreach(println)
    joinvalue.map{
      case (userid,(cmId,(name,age))) =>(cmId,List(name,age))
    }.reduceByKey(_++_).foreach(println)

    /**
      * 找出年龄大于30的顶点
      */
//    graph.vertices.filter{
//      case (id,(name,age)) =>age >= 30
//    }.foreach{
//      case (id,(name,age)) =>println(s"$name 的年龄是：$age")
//    }

    /**
      * 找出边大于5的边
      */
//    graph.edges.filter(e=>e.attr>5).foreach{e=>{
//      println(s"${e.srcId} to ${e.dstId} is ${e.attr}")
//    }}

    /**
      * 找出变属性大于5的triplts
      */
   // graph.triplets.filter(e=>e.attr>5).foreach(e=>println(s"${e.srcAttr._1} to ${e.dstAttr._1} is ${e.attr}"))
   // graph.triplets.filter(e=>e.attr>5).foreach(e=>println(s"${e.srcId} to ${e.dstId} is ${e.attr}"))

    /**
      * 找出图中最大的出度、入度、度数："
      */
//    def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int)={
//      if (a._2 > b._2) a else b
//    }
//
//    println("最大的出度"+graph.outDegrees.reduce(max))
//    println("最大的入读"+graph.inDegrees.reduce(max))
//    println("最大的度数"+graph.degrees.reduce(max))

   // graph.triplets.foreach(println)

    /**
      * 转换操作
      * 顶点的转换操作，顶点 age + 100："
      */
//    graph.mapVertices{
//      case (id,(name,age))=>(id,(name,age+100))
//    }.vertices.foreach(println)

    /**
      * 转换操作
      * 边的转换操作，边 *2 100："
      */
    //graph.mapEdges(e=>e.attr*10).edges.foreach(println)

    /**
      * 顶点年纪>30 的子图
      */
    //graph.subgraph(vpred=(id,vd)=>vd._2>30).vertices.foreach(println)
    //graph.subgraph(vpred=(id,vd)=>vd._2>10).edges.foreach(println)


    /**
      * 连接操作
      */
//    val userGraphx: Graph[User, Int] = graph.mapVertices {
//      case (id, (name, age)) => User(name, age, 0, 0)
//    }
//    val joinGraph = userGraphx.outerJoinVertices(userGraphx.inDegrees) {
//      case (id, u, inDegrees) => User(u.name, u.age, inDegrees.getOrElse(0), u.outDeg)
//    }.outerJoinVertices(userGraphx.outDegrees) {
//     case (id, user, outDegrees) => User(user.name, user.age, user.inDeg, outDegrees.getOrElse(0))
//
//    }
    //joinGraph.vertices.foreach(println)

    //出度和入读相同的人员
    //joinGraph.vertices.filter(da=>da._2.inDeg>da._2.outDeg).foreach(println)

    /**
      * 聚合操作
      * 找出年纪最大的追求者
      */
    //graph


  }
}

case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
