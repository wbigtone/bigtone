package com.haizhi.algorithm

import com.haizhi.utils.Parser._
import com.haizhi.common.Configure._
/**
  * Created by haizhi on 19/7/19.
  */
class TestChongQing extends AlgorithmMiner with Serializable{
  /**
    * 1、一周数据为mongodump图构建,重庆不替换保存到chongqing17
    * 2、一周删除边key值为/home/dig/graph_data/correct_data/a_invest目录一周总和
    * 3、全量数据为上一周融合总和
    * 4、保存目录和全量目录同级修改日期
    */
  override def miner(): Unit = {
    val sc = getSparkContext(name())
    //读取一周的图构建数据,包涵删除边的数据。(可dump跑一周,也可在chongqing目录下将7天数据union到一起)
    val oneWeekRdd = sc.textFile("/user/graph_builder/data/mongo-whole/chongqing17/parsed_20190726_09/image/invest_txt")
    val oneWeekKeyRdd =oneWeekRdd.map(line =>{
      val jv = parseJson(line)
      val key = getJsonValue(jv, "_key")
      key
    })
    //将其他删边边的key值融合为delete.txt
    val deleteRdd = sc.textFile("/user/graph_builder/data/mongo-whole/chongqing17/delete.txt")
    //求差集排出删除边,构建为(key,"1")
    val subtractRdd = oneWeekKeyRdd.subtract(deleteRdd).map(m =>(m,"1"))
    //读取一周的图构建数据构建为(key,json)
    val tupleRdd = oneWeekRdd.map(line =>{
      val jv = parseJson(line)
      val key = getJsonValue(jv, "_key")
      (key,line)
    })
    //join求出一周的增量数据
    val week_result_rdd =tupleRdd.join(subtractRdd).map{case(key,(line,num)) =>{
      (key,line)
    }}
    //到此处一周数据已处理干净,开始和总数据融合whole_rdd
    val whole_rdd = sc.textFile("/user/graph_builder/data/mongo-whole/result_whole/parsed_20190718_22/image/invest_txt").map(line =>{
      val jv = parseJson(line)
      val key = getJsonValue(jv, "_key")
      (key,line)
    })
    //求一周数据和总量数据有重合的join交集,得到key有变更的数据
    val whole_change_rdd = whole_rdd.join(week_result_rdd).map{case(key,(oldline,newline)) =>{
      key
    }}
    //全量不包涵变更的key
    val whole_subtract_result = whole_rdd.map(m =>m._1).subtract(whole_change_rdd).map(m =>(m,"1"))
    val whole_result = whole_rdd.join(whole_subtract_result).map{case(key,(line,num)) =>{
      line
    }}
    whole_result.union(week_result_rdd.map(m =>m._2)).saveAsTextFile("/user/graph_builder/data/mongo-whole/result_whole/parsed_20190725_22/image/invest_txt")
  }
}
