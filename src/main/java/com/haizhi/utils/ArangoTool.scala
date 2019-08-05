package com.haizhi.utils

import java.io.{FileInputStream, InputStreamReader}
import java.util

import com.arangodb.{ArangoDB, ArangoDBException, ArangoDatabase}
import com.mongodb.DBCursor
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by haizhi on 19/6/24.
  */
object ArangoTool {
  /**
    * 查询arango边值
    * @param query
    * @param table
    * @return
    */
  def getArangoData(query :String, table :String,arangoDB: ArangoDatabase): Set[String] = {
    var totals = Set[String]()
    val jsonResult = arangoDB.query(query.format(table),null,null,classOf[String])
    while (jsonResult.hasNext) {
      val jv: JValue = parse(jsonResult.next())
      val key = (jv \ "key").values.asInstanceOf[String]
      totals += key
    }
    totals
  }

  def getInvestData(query :String, mothod :String ,fieldString : String, table : Any*): Set[String] = {
    var totals = Set[String]()
    val cfg = new java.util.Properties
    cfg.load(new InputStreamReader(new FileInputStream("conf/arangodb.properties"), "utf-8"))

    val host = cfg.getProperty("arangodb.host")
    val port = cfg.getProperty("arangodb.port").toInt
    val user = cfg.getProperty("arangodb.username")
    val password = cfg.getProperty("arangodb.password")
    val db = cfg.getProperty("arangodb.database")
    val fields: Array[String] = fieldString.split(",")

    val arangoClient = new ArangoDB.Builder()
      .host(host,port)
      .user(user)
      .password(password)
      .build()
    val arangoDB = arangoClient.db(db)
    val jsonResult = arangoDB.query(query.format(table),null,null,classOf[String])
    while (jsonResult.hasNext) {
      val jv: JValue = parse(jsonResult.next())
      for(i <- 0 until fields.length){
        mothod match{
          case "officer" =>{
            val name = (jv \ fields(0)).values
            val position = (jv \ fields(1)).values
            if(name != null && position != null){
              totals += Array(name.asInstanceOf[String],position.asInstanceOf[String]).mkString("#0#")
            }
          }
          case "belong" =>{
            val company = (jv \ fields(0)).values
            if(company != null){
              totals += company.asInstanceOf[String]
            }
          }
          case "invest" =>{
            val company = (jv \ fields(0)).values
            if(company != null){
              totals += company.asInstanceOf[String]
            }
          }
          case "legal_man" =>{
            val person = (jv \ fields(0)).values
            if(person != null){
              totals += person.asInstanceOf[String]
            }
          }
        }

      }
    }
    arangoClient.shutdown()
    totals
  }
  /**
    * 查询arango单表属性
    * @param query
    * @param table
    * @return
    */
  def getArangoContent(query :String, table :String,business_status :String,arangoDB: ArangoDatabase): String = {
    var key=""
    val jsonResult = arangoDB.query(query.format(table),null,null,classOf[String])
    while (jsonResult.hasNext) {
      val jv: JValue = parse(jsonResult.next())
      if(jv.children.size>0){
        println("jv= "+jv)
        key = (jv \ business_status).values.asInstanceOf[String]
      }
    }
    key
  }
  /**
    * 根据keys值批量删除arango数据
    * @param dealChangeResult
    * @param collectionName
    * @tparam T
    */
  def deleteArangoBatch[T](dealChangeResult: util.ArrayList[String], collectionName:String): Unit = {
    val prop = new java.util.Properties
    prop.load(new InputStreamReader(new FileInputStream("conf/arangodb.properties"), "utf-8"))
    val arangoClient = new ArangoDB.Builder()
      .host(prop.getProperty("arangodb.host"), (prop.getProperty("arangodb.port")toInt)
      ).user(prop.getProperty("arangodb.username"))
      .password(prop.getProperty("arangodb.password"))
      .build()
    val arangoDB = arangoClient.db(prop.getProperty("arangodb.database"))
    val arangoCol = arangoDB.collection(collectionName)
    try {
      if(!dealChangeResult.isEmpty){
        arangoCol.deleteDocuments(dealChangeResult)
      }
    } catch {
      case e: ArangoDBException =>
        if (e.getErrorNum != 1202) {
          arangoClient.shutdown()
          System.exit(-1)
        }
    }
    arangoClient.shutdown()
  }
  /**
    * 根据keys值逐条arango数据
    * @param dealChangeResult
    * @param collectionName
    * @tparam T
    */
  def deleteArango[T](dealChangeResult: Set[String], collectionName:String): Unit = {
    val prop = new java.util.Properties
    prop.load(new InputStreamReader(new FileInputStream("conf/arangodb.properties"), "utf-8"))
    val arangoClient = new ArangoDB.Builder()
      .host(prop.getProperty("arangodb.host"), (prop.getProperty("arangodb.port")toInt)
      ).user(prop.getProperty("arangodb.username"))
      .password(prop.getProperty("arangodb.password"))
      .build()
    val arangoDB = arangoClient.db(prop.getProperty("arangodb.database"))
    val arangoCol = arangoDB.collection(collectionName)
    try {
      if(!dealChangeResult.isEmpty){
        for(key <- dealChangeResult){
          //           arangoCol.deleteDocument(key)
        }
      }
    } catch {
      case e: ArangoDBException =>
        if (e.getErrorNum != 1202) {
          arangoClient.shutdown()
          System.exit(-1)
        }
    }
    arangoClient.shutdown()
  }
}
