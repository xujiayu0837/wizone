package org.apache.spark

import java.text.SimpleDateFormat
import java.util.{Calendar, Timer, TimerTask}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xujiayu on 17/4/20.
  */
object QA {
  val locMacMap = mutable.Map("1" -> ("14E4E6E16E7A", "14E4E6E1867A"))
  val PREFIX = "/tmp/idea_print/spark_1_2/170424_test_"

  def m_0(groupid: Int): Unit = {
    val visitPath = new StringBuilder
    visitPath.append(PREFIX).append(groupid)
    println("m_0"+" "+visitPath)
  }
  def m_1(groupid: Int): Unit = {
    val visitPath = new StringBuilder
    visitPath.append(PREFIX).append(groupid)
    println("m_1"+" "+visitPath)
  }
  def m_2(groupid: Int): Unit = {
    val delPath = new StringBuilder
    val savePath = new StringBuilder
    delPath.append(PREFIX).append(groupid)
    savePath.append(PREFIX).append(groupid)
    println("m_2"+" "+delPath + " " + savePath)
  }
  class MyTimerTask(groupid: Int) extends TimerTask {
    override def run() = {
      m_0(groupid)
      m_1(groupid)
      m_2(groupid)
    }
  }

  /**
    *
    * @param dataPath HDFS数据的路径
    * @param locStr 监测场所
    * @return 路径字符串
    */
  def getPaths(dataPath: StringBuilder, locStr: String = ""): String = {
    // 路径字符串
    val multiPaths = new StringBuilder
    // 存储相应日期的文件路径
    val arr_0 = new ArrayBuffer[String]()
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    // 获取locStr对应AP的数据
//    var arr_1 = locMacMap.getOrElse(locStr, ()).toString.split("[^-\\w]+")
    var arr_1 = MyUtils.str2arr(locMacMap.getOrElse(locStr, ()).toString, "(", ")", ",")
    println("arr_1: " + arr_1.length)
    // locStr合法,能获取到对应AP
    if (!arr_1.isEmpty) {
//      arr_1 = arr_1.tail
      for (item <- arr_1) {
        val tmpPath = new StringBuilder
        tmpPath.append(dataPath).append(item).append("/").append(sdf.format(cal.getTime))
        arr_0 += tmpPath.toString
      }
      multiPaths.append(arr_0.mkString(","))
      return multiPaths.toString()
    }
    // 非法locStr返回空字符串
    else return ""
  }
  def concatStr(dataPath: String): String = {
    var path = new StringBuilder
    val dataPathBuf = new StringBuilder(dataPath)
    path = if (dataPathBuf.endsWith("/")) dataPathBuf.append("*/") else dataPathBuf.append("/*/")
    path = path.append("20170422")
    path.toString
  }
  def stripChars(s: String, ch: String): String = {
    val str = s.filterNot(ch.contains(_))
    str
  }
  def str2arr(str: String): Array[String] = {
    val arr = str.split("[^-\\w]+").tail
    arr
  }
  def main(args: Array[String]): Unit = {
    val str_1 = new StringBuilder("hdfs://10.103.24.161:9000/scandata/")
    val str_2 = getPaths(str_1, "10")
    println("str_2: " + str_2)
    val str_0 = concatStr("hdfs://10.103.24.161:9000/scandata/")
    println("str_0: " + str_0)

//    for (i <- (1 until(20))) {
//      val timer = new Timer()
//      val task_2 = new MyTimerTask(i)
//      timer.schedule(task_2, 0L, 5*1000L)
//    }

//    val timer = new Timer()
//    val task_0 = new TimerTask {
//      override def run() = {
//        println("*"*20)
//      }
//    }
//    val task_1 = new TimerTask {
//      override def run() = {
//        println("#"*20)
//      }
//    }
//    timer.schedule(task_0, 0L, 5*1000L)
//    timer.schedule(task_1, 0L, 5*1000L)
//    val str = locMacMap.getOrElse("1", ()).toString
//    println("str: " + str)
//    val arr_0 = stripChars(stripChars(str, "("), ")").split(",")
//    for (item <- arr_0) {
//      println("arr_0: " + item)
//    }
//    val arr_1 = str2arr(str)
//    for (item <- arr_1) {
//      println("arr_1: " + item)
//    }
  }
}
