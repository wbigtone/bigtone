package com.test.utils

import java.io.{FileInputStream, InputStreamReader}

import com.haizhi.utils.{ArangoTool, Utils}

/**
  * Created by haizhi on 19/7/26.
  */
object TestArangoTool {
  val prop = new java.util.Properties
  def main(args: Array[String]) {
    prop.load(new InputStreamReader(new FileInputStream("conf/arangodb.properties"), "utf-8"))
    prop.load(new InputStreamReader(new FileInputStream("conf/config.properties"), "utf-8"))
//    val host = prop.getProperty("arangodb.host")
//    val port = Integer.parseInt(prop.getProperty("arangodb.port"))
//    val user = prop.getProperty("arangodb.username")
//    val password = prop.getProperty("arangodb.password")
//    val db = prop.getProperty("arangodb.database")
//    val arangoClient = new ArangoDB.Builder()
//      .host(host,port)
//      .user(user)
//      .password(password)
//      .build()
//    val arangoDB = arangoClient.db(db)
//    val query_to =
//      s"""
//         |for com in Company
//         |FILTER com._id == "Company/28DE8EA30FC6E762F33931AD26C065B0"
//         |RETURN {business_status:com.business_status}
//            """.stripMargin
//    val business_status = ArangoTool.getArangoContent(query_to,"Company","business_status",arangoDB)
//    val query_from =
//      s"""
//         |for com in Company
//         |FILTER com._id == "Company/28DE8EA30FC6E762F33931AD26C065B0"
//         |RETURN {business_status:com.business_status}
//            """.stripMargin
//    val business_status_from = ArangoTool.getArangoContent(query_from,"Company","business_status",arangoDB)
//    println(business_status_from)
//    val business_status_from = "存续"
//    val business_status = "存续（在营、开业、在册）"
//    val business_online = prop.getProperty("business_online", "null_word")
//    if(business_online.contains(business_status)&&business_online.contains(business_status_from)){
//      println("AAA")
//    }
//    for(online <- business_online){
//      if(online.equals(business_status)){
//        println("AAA")
//      }
//    }
//    arangoClient.shutdown()
    val key = Utils.md5("%s%s%s".format("","",""))
    println("KEY = " +key)
  }
}
