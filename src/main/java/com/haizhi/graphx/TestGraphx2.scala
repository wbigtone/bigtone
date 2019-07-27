package com.haizhi.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by haizhi on 19/5/31.
  */
object TestGraphx2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("appName")
    val sc = new SparkContext(conf)
    //顶点(顶点id,顶点属性)
    val vertexArray = Array(
      (-1L,("-1L",20)),
      (2L,("2L",21)),
      (3L,("3L",22)),
      (4L,("4L",23)),
      (5L,("5L",24)),
      (6L,("6L",25))
    )
    //边(两个顶点id和边属性)
    val edgeArray = Array(
      Edge(2L,-1L,5),
      Edge(-1L,3L,6),
      Edge(-1L,4L,7),
      Edge(4L,6L,8),
      Edge(5L,6L,9)
    )
    val vertexRdd: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRdd: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val graph_new = Graph.fromEdges(edgeRdd,0L)

    val sccVertex = graph_new.connectedComponents().vertices

//    val leaderV01 = graph_new.triplets.map(tr => tr.srcId -> tr)
//      .leftOuterJoin(graph_new.inDegrees)
//      .flatMap{case(srcId, (tr, deg)) => {
//        if (deg.isDefined)
//          Iterator.empty
//        else {
//          Iterator(srcId -> tr)
//        }
//      }}.map(x => x._1 -> -1).distinct()

//    leaderV01.foreach(println)
//    val cc = graph_new.inDegrees.leftOuterJoin(leaderV01)
//    println("aaaa=="+cc.collect().mkString)

    val users = vertexRdd.map { line =>
      (line._1.toLong, line._2)
    }
    val ccByUsername = users.join(sccVertex).map {
      case (id, (username, cc)) => (username, cc)
    }
    println("bbbb=="+ccByUsername.collect().mkString)

//    val result = graph_new.mapVertices{case(vid,_) =>vid}
//
//    val graph: Graph[(String,Int), Int] = Graph(vertexRdd,edgeRdd)
//    graph.vertices.filter(f =>f._2._2 > 23).foreach(f =>println(s"vertices:${f._1},${f._2}"))
//    graph.edges.filter(f =>f.attr>6 && f.dstId>1L & f.srcId>1L).foreach(f =>println(s"vertices:${f.attr},${f.srcId},${f.dstId}"))
//    graph.triplets.foreach(t =>println(s"triplets:${t.srcId},${t.dstId},${t.srcAttr},${t.dstAttr},${t.attr}"))
//
//    //图的出度入度
//    graph.inDegrees.foreach(println)
//    graph.outDegrees.foreach(println)
//    graph.degrees.foreach(println)
//
//    val subGraph = graph.subgraph(vpred = (id,vid) => vid._2> 20)
//    //子图所有顶点
//    subGraph.vertices.collect().foreach(println)
//    //子图所有边
//    subGraph.edges.collect().foreach(println)
  }
}
