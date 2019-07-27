package com.haizhi.utils

import java.io.{FileInputStream, InputStreamReader}
import java.util

import com.arangodb.{ArangoDB, ArangoDBException, ArangoDatabase}
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
  /**
    * 查询arango单表属性
    * @param query
    * @param table
    * @return
    */
  def getArangoContent(query :String, table :String,business_status :String,arangoDB: ArangoDatabase): String = {
    val jsonResult = arangoDB.query(query.format(table),null,null,classOf[String])
    val jv: JValue = parse(jsonResult.next())
    val key = (jv \ business_status).values.asInstanceOf[String]
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
