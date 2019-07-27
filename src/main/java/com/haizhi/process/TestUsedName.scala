package com.haizhi.process

import java.util.Properties

import com.arangodb.ArangoDB
import com.haizhi.common.Configure.{log => _, _}
import com.haizhi.utils.Parser._
import com.haizhi.utils.{ArangoTool, Utils}
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._

import scala.collection.mutable.ListBuffer
import scala.reflect.io.File
/**
  * Created by haizhi on 19/7/19.
  */
object TestUsedName {
  var cfg = new Properties()
  var mongoClient: MongoClient = _
  def loadConfigure(): Unit = {
    try {
      Utils.usingWithClose(File("conf/mongo.properties").inputStream())(cfg.load)
      Utils.usingWithClose(File("conf/arangodb.properties").inputStream())(cfg.load)
      Utils.usingWithClose(File("conf/config.properties").inputStream())(cfg.load)
    } catch {
      case e: Throwable => log.warn(e)
    }
  }
  def main(args: Array[String]) {
    loadConfigure()
    val conf = new SparkConf().setMaster("local").setAppName("appName")
    val sc = new SparkContext(conf)
    val mongo_info = Map("mongodb.username"->cfg.getProperty("mongodb.username"),
      "mongodb.authdb"->cfg.getProperty("mongodb.authdb"),
      "mongodb.password"->cfg.getProperty("mongodb.password"),
      "mongodb.host"->cfg.getProperty("mongodb.host"),
      "mongodb.port"->cfg.getProperty("mongodb.port"),
      "mongodb.database"->cfg.getProperty("mongodb.database"))
    val arango_info = Map("arangodb.host"->cfg.getProperty("arangodb.host"),
      "arangodb.port"->cfg.getProperty("arangodb.port"),
      "arangodb.username"->cfg.getProperty("arangodb.username"),
      "arangodb.password"->cfg.getProperty("arangodb.password"),
      "arangodb.database"->cfg.getProperty("arangodb.database"))
//    findBusinessStatusFromMongo(sc,mongo_info)
    findBusinessStatusFromArango(sc,arango_info)
  }

  /**
    * 从mongo查取经营状态(加密值无法获取公司名)
    *
    * @param sc
    * @param info
    */
  def findBusinessStatusFromMongo (sc: SparkContext, info: Map[String,String]) = {
    //广播mongo参数变量
    val arango_properties = sc.broadcast(info)
    var edge = sc.textFile(cfg.getProperty("invest"))
    val result = edge.repartition(cfg.getProperty("numberParation").toInt).mapPartitions(iter =>{
      var rs = new ListBuffer[String]()
      //获取广播变量
      val prop = arango_properties.value
      val appDataAuth = MongoCredential.createCredential(prop.getOrElse("mongodb.username","readme"),
        prop.getOrElse("mongodb.authdb", "app_data"),
        prop.getOrElse("mongodb.password","readme").toCharArray
      )
      mongoClient = MongoClient.apply(new ServerAddress(prop.getOrElse("mongodb.host","172.16.215.45"), Integer.parseInt(prop.getOrElse("mongodb.port","40042"))
      ), List(appDataAuth))
      val appDatabase = mongoClient.getDB(prop.getOrElse("mongodb.database", "app_data"))
      while(iter.hasNext){
        var data = iter.next()
        val jv = parseJson(data)
        val from = getJsonValue(jv,"_from")
        val to = getJsonValue(jv,"_to")
        if(from != to){
          //此处from,to为加密值,mongo查询需要公司名
          val changeInfoCol = appDatabase.getCollection("enterprise_data_gov")
          val doc = changeInfoCol.find(DBObject("company" -> from)).next()
          val business_status = doc.get("business_status").asInstanceOf[String]
          data = jsonMerge(data,"_business_status",business_status)
          rs += data
        }
      }
      rs.toIterator
    })
    result.flatMap { e => e.toIterator }.collect().take(10).foreach(println)
  }

  /**
    * 从arango获取公司实体中的经营状态
    */
  def findBusinessStatusFromArango (sc: SparkContext, info: Map[String,String]) = {
    val business_online = sc.broadcast(prop.getProperty("business_online", "null_word").split(",").map(_.trim))
    //广播mongo参数变量
    val arango_properties = sc.broadcast(info)
    var edge = sc.textFile(cfg.getProperty("invest"))
    val result = edge.repartition(cfg.getProperty("numberParation").toInt).mapPartitions(iter =>{
      var rs = new ListBuffer[String]()
      //获取广播变量
      val prop = arango_properties.value
      val host = prop.get("arangodb.host").get
      val port = Integer.parseInt(prop.get("arangodb.port").get)
      val user = prop.get("arangodb.username").get
      val password = prop.get("arangodb.password").get
      val db = prop.get("arangodb.database").get
      val arangoClient = new ArangoDB.Builder()
        .host(host,port)
        .user(user)
        .password(password)
        .build()
      val arangoDB = arangoClient.db(db)
      while(iter.hasNext){
        var data = iter.next()
        val jv = parseJson(data)
        val from = getJsonValue(jv,"_from")
        val to = getJsonValue(jv,"_to")
        if(from != to){
          val query_to =
            s"""
               |for com in Company
               |FILTER com._id == "${to}"
               |RETURN {business_status:com.business_status}
            """.stripMargin
          val business_status_to = ArangoTool.getArangoContent(query_to,"Company","business_status",arangoDB)
          val query_from =
            s"""
               |for com in Company
               |FILTER com._id == "${from}"
               |RETURN {business_status:com.business_status}
            """.stripMargin
          val business_status_from = ArangoTool.getArangoContent(query_from,"Company","business_status",arangoDB)
          println("business_status_to="+business_status_to)
          println("business_status_from="+business_status_from)
          val online = business_online.value
          //两家公司都是在营状态,则为两个公司实体,不做曾用名替换
          if(online.contains(business_status_to)&& online.contains(business_status_from)){

          }else{
            data = jsonMerge(data,"_business_status",business_status_to)
          }
          rs += data
        }
      }
      arangoClient.shutdown()
      rs.toIterator
    })
    result.flatMap { e => e.toIterator }.collect().take(10).foreach(println)
  }
}
