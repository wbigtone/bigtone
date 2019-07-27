package com.haizhi.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Created by haizhi on 19/7/24.
  */
object TestGraphx {
  def main(args: Array[String]) {
    val graph = init()
    baseDeal(graph)
//    testTriplet(graph)
  }

  /**
    * 初始化图
    * @return
    */
  def init() : Graph[(String, String), String]= {
    val conf = new SparkConf().setMaster("local").setAppName("appName")
    val sc = new SparkContext(conf)
    //顶点(顶点id,顶点属性)
    val vertexArray =Array(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")),
      (2L, ("istoica", "prof")))
    //边(两个顶点id和边属性)
    val edgeArray = Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi"))
    //VertexId type为long,构建(VertexId, (String, String))即为[VertexId,VD]顶点类型
    val users: RDD[(VertexId, (String, String))] =  sc.parallelize(vertexArray)
    //Edge[String]默认srcId[VertexId]=0,dstId[VertexId]0
    val relationships: RDD[Edge[String]] = sc.parallelize(edgeArray)
    //默认值类型为VD,即顶点属性
    val defaultUser = ("John Doe", "Missing")
    // 创建图,调用默认构造函数apply
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)
    graph
  }

  /**
    * graphx图基础操作
    * @param graph
    */
  def baseDeal(graph : Graph[(String, String), String]) ={
    // 统计所有用户user是postdocs
    //graph.vertices extends RDD[(graphx.VertexId, VD)]
    val vertices_count: VertexId = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count()
    // 统计边值srcId大于dstId
    //extends RDD[Edge[ED]]
    val edges_count: VertexId = graph.edges.filter(e => e.srcId > e.dstId).count()
    //求图的入度,入度为指向顶点的方向,计算结果为顶点A入度X
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.foreach(println)
    //求图的出度,出度为此顶点指向其他节点的方向,计算结果为顶点A出度X
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    outDegrees.foreach(println)
    //自定义mapUdf函数,重新构建新图不保留索引
    val newVertices = graph.vertices.map { case (id, attr) => (id, mapUdf(id, attr)) }
    val newGraph_withOutIndex = Graph(newVertices, graph.edges)
    newGraph_withOutIndex.vertices.foreach(println)
    //自定义mapUdf函数,重新构建新图保留索引
    val newGraph_withIndex = graph.mapVertices((id, attr) => mapUdf(id, attr))
    newGraph_withIndex.vertices.foreach(println)
    //重新构建新图,顶点属性为入度,边不变
    val inputGraph: Graph[Int, String] = graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    inputGraph.vertices.foreach(println)
    inputGraph.edges.foreach(println)
    //求PageRank初始化,每个页面权重为1.0/出度,默认每个顶点权重为1.0
    val outputGraph: Graph[Double, Double] = inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)
    outputGraph.vertices.foreach(println)
    outputGraph.edges.foreach(println)
    //图切割???
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "collab")
    graph.edges.foreach(println)
    validGraph.edges.foreach(println)
  }

  /**
    * 三元组操作
    * SELECT src.id, dst.id, src.attr, e.attr, dst.attr
    * FROM edges AS e LEFT JOIN vertices AS src, vertices AS dst
    * ON e.srcId = src.Id AND e.dstId = dst.Id
    * @param graph
    */
  def testTriplet(graph : Graph[(String, String), String]) = {
    //triplets EdgeTriplet[VD, ED] 取顶点属性和边属性,默认赋值顶点值
    val facts: RDD[String] = graph.triplets.map(triplet =>
        triplet.srcAttr._1 + ","
        + triplet.attr + ","
        + triplet.dstAttr._1 + ","
        + triplet.srcId + ","
        + triplet.dstId
    )
    facts.collect.foreach(println(_))
  }

  /**
    * 自定义函数,重新构图
    * @param id
    * @param VD
    * @return
    */
  def mapUdf(id:VertexId,VD:(String,String)) : (Long,String) = {
    (id,VD._1+"_"+VD._2)
  }
}
