package com.haizhi.work.algorithm

/**
  * Created by wangdashi on 2017/9/2.
  * 图挖掘算法公共接口
  */

trait AlgorithmMiner {

  def init(): Unit = {}

  def name(): String = this.getClass.getSimpleName

  def miner(): Unit

}
