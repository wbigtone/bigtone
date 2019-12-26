package com.haizhi.spark.graphx

import org.apache.spark.graphx.{Edge, _}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Created by haizhi on 19/7/24.
  */
object TestGraphx {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("appName")
    val sc = new SparkContext(conf)
    val graph = init(sc)
//    graphCommon(graph)
//    graphAttribute(graph)
    graphConstruct(graph)
//    graphJoin(graph)
//    graphJoinTriplets(graph)
//    graphAlgorithm(sc)
  }

  /**
    * 初始化图
    * @return
    */
  def init(sc :SparkContext) : Graph[(String, String), String]= {
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
    * 图基础属性
      val numEdges: Long
      val numVertices: Long
      val inDegrees: VertexRDD[Int]
      val outDegrees: VertexRDD[Int]
      val degrees: VertexRDD[Int]
      图属性中包涵的存储集合
      val vertices: VertexRDD[VD]
      val edges: EdgeRDD[ED]
      val triplets: RDD[EdgeTriplet[VD, ED]]
    * @param graph
    */
  def graphCommon(graph : Graph[(String, String), String]) ={
    println("边的条数:"+graph.numEdges)
    println("顶点个数:"+graph.numVertices)
    // 统计所有用户user是postdocs,过滤函数
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
    //求图的度,包涵入度和出度
    val degrees: VertexRDD[Int] = graph.degrees
    degrees.foreach(println)
    //三元组属性
    testTriplet(graph)
  }

  /**
    * 图缓存
    * def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED]
      def cache(): Graph[VD, ED]
      def unpersistVertices(blocking: Boolean = true): Graph[VD, ED]
    * @param graph
    * @return
    */
  def graphCache(graph : Graph[(String, String), String]) = {
    graph.cache()//默认MEMORY_ONLY
    graph.persist(StorageLevel.MEMORY_ONLY)//StorageLevel中设置各级缓存
    graph.unpersistVertices(true)//缓存顶点
  }

  /**
    * 图属性运算,与Rdd的map类似,可以构建成新图
      def mapVertices[VD2](map: (VertexId, VD) => VD2): Graph[VD2, ED]
      def mapEdges[ED2](map: Edge[ED] => ED2): Graph[VD, ED2]
      def mapEdges[ED2](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2]
      def mapTriplets[ED2](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]
      def mapTriplets[ED2](map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2]): Graph[VD, ED2]
    * @param graph
    */
  def graphAttribute(graph : Graph[(String, String), String]) ={
    //自定义mapUdf函数,重构顶点属性,新图不保留索引
    val newVertices = graph.vertices.map { case (id, attr) => (id, mapUdf(id, attr)) }
    val newGraph_withOutIndex = Graph(newVertices, graph.edges)
    newGraph_withOutIndex.vertices.foreach(println)
    //自定义mapUdf函数,重构顶点属性,新图保留索引
    val newGraph_withIndex = graph.mapVertices((id, attr) => mapUdf(id, attr))
    newGraph_withIndex.vertices.foreach(println)
    //重构边值
    val edgesGraph: Graph[(String, String), String] = graph.mapEdges(graph =>graph.attr+"_graphx")
    edgesGraph.edges.foreach(println)
    //重新构建新图,顶点属性为入度,边不变
    val inputGraph: Graph[Int, String] = graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    inputGraph.vertices.foreach(println)
    inputGraph.edges.foreach(println)
    //求PageRank初始化,每个页面权重为1.0/出度,默认每个顶点权重为1.0
    val outputGraph: Graph[Double, Double] = inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)
    outputGraph.vertices.foreach(println)
    outputGraph.edges.foreach(println)
  }

  /**
    * 图结构运算
    * def reverse: Graph[VD, ED]
      def subgraph(epred: EdgeTriplet[VD,ED] => Boolean = (x => true), vpred: (VertexId, VD) => Boolean = ((v, d) => true)): Graph[VD, ED]
      def mask[VD2, ED2](other: Graph[VD2, ED2]): Graph[VD, ED]
      def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED]
    * @param graph
    */
  def graphConstruct(graph : Graph[(String, String), String]) = {
    //图切割,attr为顶点属性,id为顶点值
    val subgraph: Graph[(String, String), String] = graph.subgraph(vpred = (id, attr) => id != 3L)//attr._2!="student"
    subgraph.vertices.foreach(println)
    subgraph.edges.foreach(println)
    //反转图,边的指向反转
    val reverse: Graph[(String, String), String] = graph.reverse
    reverse.vertices.foreach(println)
    reverse.edges.foreach(println)
    //connectedComponents联通分支,默认将整个分支计算结果指向顶点值最小的点。剔除独立的点
    val ccGraph = graph.connectedComponents()
    ccGraph.vertices.foreach(println)
    ccGraph.edges.foreach(println)
    //删除缺失的顶点和边
//    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "student")
//    //结果集为两个图的子图
//    val validCCGraph = ccGraph.mask(validGraph)
//    validCCGraph.vertices.foreach(println)
//    validCCGraph.edges.foreach(println)
//    //roupEdges操作者合并平行边在多重图
//    val groupEdges: Graph[(String, String), String] = graph.groupEdges((edge1,edge2) =>edge1+edge2)
//    groupEdges.vertices.foreach(println)
//    groupEdges.edges.foreach(println)
  }

  /**
    * 图聚合
      def joinVertices[U](table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD): Graph[VD, ED]
      def outerJoinVertices[U, VD2](other: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, Option[U]) => VD2): Graph[VD2, ED]
    * @param graph
    */
  def graphJoin(graph : Graph[(String, String), String]) = {
    //jion操作
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    val joinGraph = graph.joinVertices(outDegrees){(id, oldAttr, outDegOpt) =>{
      oldAttr
      }
    }
    joinGraph.vertices.foreach(println)
    joinGraph.edges.foreach(println)
    //outerJoin操作
    val degreeGraph = graph.outerJoinVertices(outDegrees) { (id, oldAttr, outDegOpt) =>
      outDegOpt match {
        case Some(outDeg) => outDeg
        case None => 0 //无出度即出度为0
      }
    }
    degreeGraph.vertices.foreach(println)
    degreeGraph.edges.foreach(println)
  }

  /**
    * 聚合三元组
    * 通过在每个顶点收集相邻顶点及其属性
    * def collectNeighborIds(edgeDirection: EdgeDirection): VertexRDD[Array[VertexId]]
      def collectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, VD)]]
      GraphX中的核心聚合操作是aggregateMessages。此运算符将用户定义的sendMsg函数应用于图中的每个边三元组，
      可以将其sendMsg视为 map-reduce中的map函数,用户定义的mergeMsg函数接收两个发往同一顶点的消息，
      并产生一条消息。可以认为是map-reduce中mergeMsg的reduce函数
      aggregateMessages操作者返回一个VertexRDD[Msg] 包含该聚合消息（类型的Msg）发往每个顶点。
      未收到消息的顶点不包含在返回的VertexRDDVertexRDD中
      然后使用该mergeMsg函数在其目标顶点聚合这些消息。
      此外，还有aggregateMessages一个可选项 tripletsFields，
      用于指示访问哪些数据EdgeContext （即源顶点属性但不是目标顶点属性）。
      def aggregateMessages[Msg: ClassTag](
          sendMsg: EdgeContext[VD, ED, Msg] => Unit,
          mergeMsg: (Msg, Msg) => Msg,
          tripletFields: TripletFields = TripletFields.All)
      : VertexRDD[A]
    */
  def graphJoinTriplets(graph : Graph[(String, String), String]) = {
    //case EdgeDirection.In,EdgeDirection.Out,EdgeDirection.Either,EdgeDirection.Both
    val collectNeighbors: VertexRDD[Array[(VertexId, (String, String))]] = graph.collectNeighbors(EdgeDirection.In)
    collectNeighbors.foreach(println)
    val collectNeighborIds: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)
    collectNeighborIds.foreach(println)
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr._2.length > triplet.dstAttr._2.length) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst((1, triplet.dstId))
        }
      },
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    olderFollowers.foreach(println)
  }

  /**
    * 图算法 pregel,pageRank,联通分支,三角形计数,强联通分支
    * def pregel[A](initialMsg: A, maxIterations: Int, activeDirection: EdgeDirection)(
          vprog: (VertexId, VD, A) => VD,
          sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId,A)],
          mergeMsg: (A, A) => A) : Graph[VD, ED]
      def pageRank(tol: Double, resetProb: Double = 0.15): Graph[Double, Double]
      def connectedComponents(): Graph[VertexId, ED]
      def triangleCount(): Graph[Int, ED]
      def stronglyConnectedComponents(numIter: Int): Graph[VertexId, ED]
    * @param sc
    */
  def graphAlgorithm(sc :SparkContext): Unit ={
    testPregel(sc)
  }

  /**
    * 测试 Pregel
    * @param sc
    */
  def testPregel(sc: SparkContext) = {
    val edgeArray = Array(
      Edge(1L, 2L, 1),
      Edge(2L, 6L, 1),
      Edge(3L, 6L, 1),
      Edge(6L, 1L, 1),
      Edge(6L, 3L, 1))
    val edgeRdd: RDD[Edge[PartitionID]] = sc.parallelize(edgeArray)
    val graph: Graph[VertexId, PartitionID] = Graph.fromEdges(edgeRdd,0L)
    val newGraph = graph.mapVertices{case(vid,_) => vid}
      .pregel(0L,Int.MaxValue)(
      vprog = (vid :VertexId, vattr :Long, mess :Long) =>math.max(vattr,mess),
      sendMsg = (tr :EdgeTriplet[Long, Int]) =>{
        if(tr.srcAttr < tr.dstAttr)
          Iterator(tr.srcAttr -> tr.dstAttr)
        else if(tr.srcAttr > tr.dstAttr)
          Iterator(tr.dstAttr -> tr.srcAttr)
        else
          Iterator empty
      },
      mergeMsg = (mess1 :Long, mess2 :Long) => math.max(mess1,mess2)
      )
    newGraph.vertices.collect().foreach(println)
  }

  /**
    * 测试pageRank
    */
  def testPageRank() = {

  }

  /**
    * 测试联通分支
    */
  def testConnectedComponents() = {

  }

  /**
    * 测试三角形计数
    */
  def testTriangleCount() = {

  }

  /**
    * 测试强联通分支
    */
  def testStronglyConnectedComponents = {

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
