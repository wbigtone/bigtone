package com.haizhi.work.algorithm

import org.apache.log4j.{LogManager, Logger}

/**
  * Created by wangdashi on 2017/9/2.
  * 图挖掘算法入口
  */
object AlgorithmMinerEntry {

  private val log: Logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    if(args.isEmpty){
      log.error(s"There are no any args, please entry a algorithm name.")
    }

    val algorithmName = args.head
    val minerClassName = s"com.haizhi.graph.algorithm.$algorithmName"
    var miner: AlgorithmMiner = null
    try {
      miner = Class.forName(minerClassName).newInstance().asInstanceOf[AlgorithmMiner]
    } catch {
      case e: java.lang.ClassNotFoundException =>
        log.error(s"算法挖掘功能类${minerClassName}不存在\n"+e.fillInStackTrace)
      case e: java.lang.ClassCastException =>
        log.error(s"算法挖掘功能类${minerClassName}没有继承com.haizhi.graph.algorithm.AlgorithmMiner\n"
          +e.fillInStackTrace)
      case e: Exception => log.error(e.fillInStackTrace())
    }

    log.info(s"Start miner $algorithmName.")

    miner.miner()

    log.info(s"Finish miner $algorithmName.")

  }

}
