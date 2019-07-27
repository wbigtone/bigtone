package com.haizhi.utils

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date
import java.util.regex.Pattern

import org.apache.log4j.{LogManager, Logger}
import org.json4s.JsonAST.{JField, JNothing, JObject}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization
import org.json4s.{Diff, JValue, ShortTypeHints}

/**
  * Created by dashi on 2019-07-25
  */
object Parser {
  val log: Logger = LogManager.getLogger(getClass)

  // json easy interface
  def getJsonValue(obj: JValue, key: String, default: String = ""): String = {
    var rs = default
    try {
      val find_field = obj \ key
      if (find_field != JNothing) {
        rs = find_field.values.toString
      }
    } catch {
      case e: Throwable => log.warn("Invalid json [%s] value return default".format(jsonString(obj)))
    }
    rs
  }

  def checkJsonKey(obj: JValue, key: String): Boolean = {
    var rs = false
    obj findField { case (k, v) => k == key
    } match {
      case Some(v) => rs = true
      case None => rs = false
    }
    rs
  }

  def jsonString(obj: JValue): String = {
    compact(render(obj))
  }

  def jsonString(array: Array[(String, String)]): String = {
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    var str = JObject()
    array.foreach(a => str ~= a)
    compact(render(str))
  }

  def parseJson(json_str: String): JValue = {
    parse(json_str)
  }

  def jsonMerge(jsonStr: String, key: String, value: String): String = {
    var newJson = jsonStr
    try {
      val jv = parse(jsonStr)
      val newStr = compact(render(key -> value))
      val lastJson = jv merge parse(newStr)
      newJson = compact(render(lastJson))
    }
    catch {
      case ex: Exception => ex.printStackTrace()
    }
    newJson
  }

  def jsonMerge(jsonStr: String, keyValue: (String, String)*): String = {
    var newJson = jsonStr
    try {
      keyValue.foreach{case(k, v) => newJson = jsonMerge(newJson, k, v)}
    }
    catch {
      case ex: Exception => ex.printStackTrace()
    }
    newJson
  }

  def hasJsonKey(jv: JValue, key: String): Boolean = !(jv \ key).equals(JNothing)

  def buildTupleJson[B <% JValue](tuple: (String, B)): JValue = {
    JObject(JField(tuple._1, tuple._2) :: Nil)
  }

  def jsonEqual(left: JValue, right: JValue): Boolean = {
    val Diff(changed, added, deleted) = left diff right
    if (changed != JNothing) {
      false
    } else if (added != JNothing) {
      false
    } else if (deleted != JNothing) {
      false
    } else {
      true
    }
  }

  // string formatted
  def moneyFormatted(money_str: String, default: Double): JValue = {
    val default_formatted = ("value" -> default) ~ ("unit" -> "元")
    if (money_str.trim() == "") {
      return default_formatted
    }
    try {
      val items = money_str.split('(')
      val money_num = items(0).toDouble
      var money_unit = "元"
      if (items.length >= 2) {
        val unit_items = items(1).split(')')
        money_unit = unit_items(0)
      }
      return ("value" -> money_num) ~ ("unit" -> money_unit)
    } catch {
      case e: Throwable => log.warn("invalid money string format %s".format(money_str))
    }
    default_formatted
  }

  val format: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def getCurrentTime: String = format.format(new Date())

  def normCompanyName(companyName: String): String ={
    companyName.replace("(", "（").replace(")", "）")
  }

  def isNumeric(numberString:String):Boolean = {
    val numberPattern = Pattern.compile("-?[0-9]+\\.?[0-9]*")
    val matchs = numberPattern.matcher(numberString)
    matchs.matches()
    if(!matchs.matches()) false else true

  }
  def normCaseId(caseId: String): String = {
    var rs = caseId.replace('(','（')
        .replace(')','）')
        .replace('[','（')
        .replace(']','）')
        .replace('｛','（')
        .replace('｝','）')
        .replace('「','（')
        .replace('」','）')
    val num = "([0-9]+)".r
    val iter = num findAllIn rs
    while (iter.hasNext) {
      val term = iter.next()
      val ind = rs.indexOf(term)
      if (ind != -1) {
        val left_ind = ind
        val right_ind = ind + term.length
        if (right_ind <= rs.length) {
          val n = term.toLong.toString
          rs = rs.substring(0, left_ind) + n + rs.substring(right_ind)
        }
      }
    }
    rs
  }

  def trim(str: String): String = {
    str.replaceAll("\u00A0", " ").trim
  }

}
