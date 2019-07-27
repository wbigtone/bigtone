package com.test.utils

import java.io.{FileInputStream, InputStreamReader}

import com.arangodb.ArangoDB
import com.haizhi.utils.ArangoTool

/**
  * Created by haizhi on 19/7/26.
  */
object TestArangoTool {
  val prop = new java.util.Properties
  def main(args: Array[String]) {
    prop.load(new InputStreamReader(new FileInputStream("conf/arangodb.properties"), "utf-8"))
    val host = prop.getProperty("arangodb.host")
    val port = Integer.parseInt(prop.getProperty("arangodb.port"))
    val user = prop.getProperty("arangodb.username")
    val password = prop.getProperty("arangodb.password")
    val db = prop.getProperty("arangodb.database")
    val arangoClient = new ArangoDB.Builder()
      .host(host,port)
      .user(user)
      .password(password)
      .build()
    val arangoDB = arangoClient.db(db)
    val query_to =
      s"""
         |for com in Company
         |FILTER com._id == "Company/01CFBE50ED5695C9A4ADECE136BBD2ED"
         |RETURN {business_status:com.business_status}
            """.stripMargin
    val business_status = ArangoTool.getArangoContent(query_to,"Company","business_status",arangoDB)
    println(business_status)
    arangoClient.shutdown()
  }
}
