package com.haizhi.process

import java.io.{FileInputStream, InputStreamReader, PrintWriter}
import java.util.Properties

import com.arangodb.ArangoDB
import com.haizhi.utils.Parser._
import com.haizhi.utils.{ArangoTool, KeyUtils, Utils}
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import org.json4s.{JObject => _, JString => _}

import scala.reflect.io.{File, Path}

/**
  * Created by haizhi on 19/7/29.
  */
object TestUsedNamesCase {
  var cfg = new Properties()
  var mongoClient: MongoClient = _
  def loadConfigure(): Unit = {
    try {
      cfg.load(new InputStreamReader(new FileInputStream("conf/arangodb.properties"), "utf-8"))
      cfg.load(new InputStreamReader(new FileInputStream("conf/config.properties"), "utf-8"))
    } catch {
      case e: Throwable => log.warn(e)
    }
  }
  def main(args: Array[String]) {
    loadConfigure()
    val business_online = cfg.getProperty("business_online", "null_word")
    val appDataAuth = MongoCredential.createCredential(cfg.getProperty("mongodb.username","readme"),
      cfg.getProperty("mongodb.authdb", "app_data"),
      cfg.getProperty("mongodb.password","readme").toCharArray
    )
    mongoClient = MongoClient.apply(new ServerAddress(cfg.getProperty("mongodb.host","172.16.215.45"), Integer.parseInt(cfg.getProperty("mongodb.port","40042"))
    ), List(appDataAuth))
    //arango perperties
    val host = cfg.getProperty("arangodb.host")
    val port = Integer.parseInt(cfg.getProperty("arangodb.port"))
    val user = cfg.getProperty("arangodb.username")
    val password = cfg.getProperty("arangodb.password")
    val db = cfg.getProperty("arangodb.database")
    val arangoClient = new ArangoDB.Builder()
      .host(host,port)
      .user(user)
      .password(password)
      .build()
    val arangoDB = arangoClient.db(db)
    val appDatabase = mongoClient.getDB(cfg.getProperty("mongodb.database", "app_data"))
    val changeInfoCol = appDatabase.getCollection("enterprise_data_gov")
    val cursor = changeInfoCol.find(DBObject("used_name_list.0" -> new BasicDBObject("$exists", true))).limit(10000)
    var result11 = Set[String]()
//    var result12 = Set[String]()
//    var result13 = Set[String]()
//    var result14 = Set[String]()
//    var result21 = Set[String]()
//    var result22 = Set[String]()
//    var result31 = Set[String]()
//    var result32 = Set[String]()
//    var result4 = Set[String]()
    while (cursor.hasNext) {
      //现用名公司信息:公司名,经营状态,曾用名列表
      val doc = cursor.next()
      val company_name = doc.get("company").asInstanceOf[String]
      val business_status = doc.get("business_status").asInstanceOf[String]
      val company_used_names =doc.getOrElse("used_name_list", new BasicDBList()).asInstanceOf[BasicDBList]
      //遍历曾用名
      if(company_used_names.size()>1){
        for(company_used_name <- company_used_names){
          val used_name = company_used_name.asInstanceOf[String]
          if(used_name!=null && used_name.length>0){
            val cursor_used_name = changeInfoCol.find(DBObject("company" -> used_name))
            var business_status_used =""
            var company_name_used = ""
            while (cursor_used_name.hasNext){
              //曾用名公司信息:公司名,经营状态,曾用名列表
              val doc_used = cursor_used_name.next()
              company_name_used = doc_used.get("company").asInstanceOf[String]
              business_status_used= doc_used.get("business_status").asInstanceOf[String]
            }
            if(company_name_used!=null && company_name_used.length>0){
              if(business_status!=null && business_status.length>0 && business_status_used!=null && business_status_used.length>0){
                if(business_online.contains(business_status) && business_online.contains(business_status_used)){
                  val company_name_md5 = "Company/"+KeyUtils.companyKeyBuild(company_name)
                  val company_name_used_md5 = "Company/"+KeyUtils.companyKeyBuild(company_name_used)
                  val query_person_to =
                    s"""
                       |for inv in invest
                       |for per in Person
                       |FILTER inv._from == per._id
                       |FILTER inv._to == "${company_name_md5}"
                       |RETURN {name:per.name}
            """.stripMargin
                  val invest_person_to = ArangoTool.getInvestData(query_person_to,"invest","name","invest","Person")
                  val query_company_to =
                    s"""
                       |for inv in invest
                       |for com in Company
                       |FILTER inv._from == com._id
                       |FILTER inv._to == "${company_name_md5}"
                       |RETURN {name:com.name}
            """.stripMargin
                  val invest_company_to = ArangoTool.getInvestData(query_company_to,"invest","name","invest","Company")
                  val invest_to = (invest_person_to ++ invest_company_to)

                  val query_person_from =
                    s"""
                       |for inv in invest
                       |for per in Person
                       |FILTER inv._from == per._id
                       |FILTER inv._to == "${company_name_used_md5}"
                       |RETURN {name:per.name}
            """.stripMargin
                  val invest_person_from = ArangoTool.getInvestData(query_person_from,"invest","name","invest","Person")
                  val query_company_from =
                    s"""
                       |for inv in invest
                       |for com in Company
                       |FILTER inv._from == com._id
                       |FILTER inv._to == "${company_name_used_md5}"
                       |RETURN {name:com.name}
            """.stripMargin
                  val invest_company_from = ArangoTool.getInvestData(query_company_from,"invest","name","invest","Company")
                  val invest_from = (invest_person_from ++ invest_company_from)

                  val query_company_to2=
                    s"""
                       |for inv in invest
                       |for com in Company
                       |FILTER inv._to == com._id
                       |FILTER inv._from == "${company_name_md5}"
                       |RETURN {name:com.name}
            """.stripMargin
                  val invest_company_to2 = ArangoTool.getInvestData(query_company_to2,"invest","name","invest","Company")
                  val query_company_from2 =
                    s"""
                       |for inv in invest
                       |for com in Company
                       |FILTER inv._to == com._id
                       |FILTER inv._from == "${company_name_used_md5}"
                       |RETURN {name:com.name}
            """.stripMargin
                  val invest_company_from2 = ArangoTool.getInvestData(query_company_from2,"invest","name","invest","Company")

                  if(invest_to.size>0||invest_from.size>0||invest_company_to2.size>0||invest_company_from2.size>0){
                    println(company_name+"========================="+company_name_used)
                    println(" 现用名:"+company_name+" 经营状态:"+business_status+ "\n" +
                      " 曾用名:"+company_name_used+" 经营状态:"+business_status_used+"\n" +
                      " 现用名被投资:"+invest_to +" 曾用名被投资:"+ invest_from +"\n" +
                      " 现用名投资:"+invest_company_to2 +" 曾用名投资:"+ invest_company_from2 +"\n")
                    result11 += " 现用名:"+company_name+" 经营状态:"+business_status+ "\n" +
                      " 曾用名:"+company_name_used+" 经营状态:"+business_status_used+"\n" +
                      " 现用名被投资:"+invest_to +" 曾用名被投资:"+ invest_from +"\n" +
                      " 现用名投资:"+invest_company_to2 +" 曾用名投资:"+ invest_company_from2 +"\n"
                  }

                }

//                else if(business_online.contains(business_status) && !business_online.contains(business_status_used)){
//                  println("result12->现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used)
//                  result12 += "现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used
//                }else if(!business_online.contains(business_status) && business_online.contains(business_status_used)){
//                  println("result13->现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used)
//                  result13 += "现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used
//                }else{
//                  println("result14->现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used)
//                  result14 += "现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used
//                }
              }
//              else if((business_status ==null || business_status.length==0)&& business_status_used!=null&&business_status_used.length>0){
//                if(business_online.contains(business_status_used) ){
//                  println("result21->现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used)
//                  result21 += "现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used
//                }else{
//                  println("result22->现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used)
//                  result22 += "现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used
//                }
//              }else if((business_status_used ==null || business_status_used.length==0)&& business_status!=null&&business_status.length>0){
//                if(business_online.contains(business_status)){
//                  println("result31->现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used)
//                  result31 += "现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used
//                }else{
//                  println("result31->现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used)
//                  result32 += "现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used
//                }
//              }else{
//                println("result4->现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used)
//                result4 += "现用名:"+company_name+" 经营状态:"+business_status+" 曾用名列表:"+company_used_names+" 曾用名:"+company_name_used+" 经营状态:"+business_status_used
//              }
            }
          }
        }
      }
    }
    Path("/Users/haizhi/source/usedname_online_many").createDirectory()
    val output11 = new PrintWriter(File("/Users/haizhi/source/usedname_online_many/经营状态都存在且都在营").outputStream())
//    val output12 = new PrintWriter(File("/Users/haizhi/source/usedname_many/经营状态都存在现用名在营曾用名不在营").outputStream())
//    val output13 = new PrintWriter(File("/Users/haizhi/source/usedname_many/经营状态都存在现用名不在营曾用名在营").outputStream())
//    val output14 = new PrintWriter(File("/Users/haizhi/source/usedname_many/经营状态都存在且都不在营").outputStream())
//    val output21 = new PrintWriter(File("/Users/haizhi/source/usedname_many/现用名无经营状态曾用名有经营状态为在营").outputStream())
//    val output22 = new PrintWriter(File("/Users/haizhi/source/usedname_many/现用名无经营状态曾用名有经营状态为不在营").outputStream())
//    val output31 = new PrintWriter(File("/Users/haizhi/source/usedname_many/现用名有经营状态为在营曾用名无经营状态").outputStream())
//    val output32 = new PrintWriter(File("/Users/haizhi/source/usedname_many/现用名有经营状态为不在营曾用名无经营状态").outputStream())
//    val output4  = new PrintWriter(File("/Users/haizhi/source/usedname_many/现用名何曾用名都无经营状态").outputStream())
    result11.foreach(rkey => {
      output11.println(s"${rkey}")
    })
    output11.close()
//    result12.foreach(rkey => {
//      output12.println(s"${rkey}")
//    })
//    output12.close()
//    result13.foreach(rkey => {
//      output13.println(s"${rkey}")
//    })
//    output13.close()
//    result14.foreach(rkey => {
//      output14.println(s"${rkey}")
//    })
//    output14.close()
//    result21.foreach(rkey => {
//      output21.println(s"${rkey}")
//    })
//    output21.close()
//    result22.foreach(rkey => {
//      output22.println(s"${rkey}")
//    })
//    output22.close()
//    result31.foreach(rkey => {
//      output31.println(s"${rkey}")
//    })
//    output31.close()
//    result32.foreach(rkey => {
//      output32.println(s"${rkey}")
//    })
//    output32.close()
//    result4.foreach(rkey => {
//      output4.println(s"${rkey}")
//    })
//    output4.close()
  }
}
