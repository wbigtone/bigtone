package com.haizhi.common

import java.io.{File, FileInputStream}
import java.io.InputStreamReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoujiamu on 2017/8/25.
  */

object Configure {

  val log: Logger = LogManager.getLogger(getClass)
  private val config = if (new File("mining-algorithm.properties").exists())
    "config.properties"
  else
    "conf/config.properties"

  val prop = new java.util.Properties
  prop.load(new InputStreamReader(new FileInputStream(config), "utf-8"))
  val sparkHbaseHost: String = prop.getProperty("sparkHbaseHost")
  val HBaseClientPort: String = prop.getProperty("HBaseClientPort", "2181")
  val nameSpace: String = prop.getProperty("nameSpace")
  val numPartitions: Int = prop.getProperty("numPartitions", "300").toInt

  def getSparkContext(appName: String, setKeyVal: (String, String)*): SparkContext = {
    val master: String = prop.getProperty("sparkMaster")
    val conf: SparkConf = new SparkConf(true)
        .setMaster(master)
        .setAppName(appName)
        .set("spark.hbase.host", sparkHbaseHost)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.rdd.compress", "true")
    if (setKeyVal.nonEmpty)
      setKeyVal.foreach{case(k, v) => conf.set(k, v)}
    new SparkContext(conf)
  }



  def clearPath(sc: SparkContext, outFile: String): Unit = {
    //若结果文件已存在，删除它
    val hadoopConf: Configuration = sc.hadoopConfiguration
    val hdfs: FileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new org.apache.hadoop.fs.Path(outFile)
    if (hdfs.exists(path)) {
      val result = hdfs.delete(path, true)
      if (result)
        log.info(s"""Path "$outFile" have been deleted successfully.""")
      else
        log.warn(s"""Path "$outFile" deleted field.""")
    }else
      log.info(s"""Path "$outFile" does not exist, don't need to delete.""")
  }
}
