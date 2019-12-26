package com.haizhi.utils

import java.security.MessageDigest
import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag
/**
	* Created by wangdashi on 2019-07-25
	*/
object Utils {

	private val dateFormat = new ThreadLocal[SimpleDateFormat] {
		override def initialValue = {
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		}
	}

	private val dayFormat = new ThreadLocal[SimpleDateFormat] {
		override def initialValue = {
			new SimpleDateFormat("yyyy-MM-dd")
		}
	}

	def currentDateToString(format: String): String = {
		val now: Date = new Date()
		val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
		val date = dateFormat.format(now)
		date
	}

	def getYesterday():String= {
		var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		var cal: Calendar = Calendar.getInstance()
		cal.add(Calendar.DATE, -1)
		var yesterday = dateFormat.format(cal.getTime())
		yesterday
	}

	def usingWithClose[A <: {def close():Unit}, B](resource:A)(f: A => B): B =
		try {
			f(resource)
		} finally {
			resource.close()
		}

	private val YEAR_SECONDS = 365 * 24 * 3600 * 1000L

	def uniqByKey[T:ClassTag](d: RDD[T])(f: T => String, m: (T, T) => T): RDD[T] = {
        d.map(e => (f(e), e))
          .reduceByKey(m)
          .map{case (k, v) => v}
    }

	def uniqByKey[T:ClassTag](d: List[T])(f: T => String, m: (T, T) => T): List[T] = {
		d.map(e => (f(e), e))
				.groupBy(_._1)
				.map(l => l._2.map(_._2).reduce((s, t) => m(s, t)))
	        	.toList
	}

    def createEmptyRDD[T: ClassTag](sc: SparkContext): RDD[T] = {
        return sc.parallelize(Array[T]())
    }

    def createEmptyGraph[T: ClassTag](sc: SparkContext, default: T): Graph[T, T] = {
        var vertices: RDD[(VertexId, T)] = sc.parallelize(Array[(VertexId, T)]())
        var edges: RDD[Edge[T]] = sc.parallelize(Array[Edge[T]]())
        return Graph(vertices, edges, default)
    }

    def graphId(id: String): Long = {
        BigInt(id.substring(0, 16), 16).toLong
    }

    def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

    def md5(s: String): String = {
        val bytes = MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8"))
        val hex_digest = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
        val rs = new StringBuffer()
        for (i <- 0.to(15)) {
            rs.append(hex_digest((bytes(i) >>> 4 & 0xf)))
            rs.append(hex_digest((bytes(i) & 0xf)))
        }
        rs.toString
    }

	def curDateStr(): String = {
		var time_str = new SimpleDateFormat("YYYYMMdd") format new Date().getTime()
		time_str
	}

	def curDateCp(current: Long, date: String): Boolean = {
		var timeDiff: Long = 0L
		try {
			//处理工商表里注册时间大于当前时间的脏数据,但不能处理为什么会数据正常也会出现负值的情况
			timeDiff = current - dateFormat.get().parse(date).getTime
		} catch {
			case ex: ParseException => {
				timeDiff = current - dayFormat.get().parse(date).getTime
			}
		}
		//abs
		if (timeDiff < 0) timeDiff = -timeDiff
		timeDiff < YEAR_SECONDS
	}

	def curDateDif(current: Long, date: String): String = {
		var timeDiff: Long = 0L
		try {
			//处理工商表里注册时间大于当前时间的脏数据,但不能处理为什么会数据正常也会出现负值的情况
			timeDiff = current - dateFormat.get().parse(date).getTime
		} catch {
			case ex: ParseException => {
				timeDiff = current - dayFormat.get().parse(date).getTime
			}
		}
		if (timeDiff < 0) {
			return "未知"
		}
		(timeDiff / YEAR_SECONDS.toDouble).toString
	}

	def main(args: Array[String]) {
		val date = "1998-08-10"
		println(curDateDif(System.currentTimeMillis(), date))
	}
}


class UnionFinder extends Serializable{
	val fatherSet = new mutable.HashMap[String, String]()
	def findFather(x:String):String={
		var x_temp = x
		var xFather = fatherSet.getOrElse(x, x)
		var cnt = 0
		var travList: List[String] = List()
		while(x_temp != xFather && xFather != null){
			cnt += 1
			travList :+= x_temp
			x_temp = xFather
			xFather = fatherSet.getOrElse(x_temp, x_temp)
		}
		if(cnt >= 2){
			travList.foreach(e=>fatherSet.update(e, xFather))
		}
		xFather
	}
	def unionFather (x:String, y:String):Unit={
		val xFather = findFather(x)
		val yFather = findFather(y)
		if (xFather != yFather) {
			fatherSet.update(xFather, yFather)
		}
	}
	def getFather(x:String):String = findFather(x)
}

object UnionFinder{
	def apply(edges:Array[(String, String)]): UnionFinder = {
		val unionFinder = new UnionFinder()
		edges.foreach { case(s, d) =>
			unionFinder.fatherSet.update(s, s)
			unionFinder.fatherSet.update(d, d)
		}
		edges.foreach { case (s, d) =>
			unionFinder.unionFather(s, d)
		}
		unionFinder
	}
}