package com.test.algorithm

import com.haizhi.utils.Parser._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by haizhi on 19/7/26.
  */
object TestActualController {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("appName")
    val sc = new SparkContext(conf)
    sc.textFile("/Users/haizhi/chongqing/actual_controller").map(line =>{
      val jv = parseJson(line)
      val from = getJsonValue(jv, "_from")
      if(from.split("/").head.equals("Company")){
        from
      }else{
        "0"
      }
    }).filter(f =>f.contains("Company")).take(20).foreach(println)
  }
}
