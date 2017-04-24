package org.apache.spark

import java.io.File
import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xujiayu on 17/4/18.
  */
object MyUtils {
  val locMacMap = mutable.Map("1" -> ("14E4E6E16E7A", "14E4E6E1867A"), "2" -> ("14E4E6E17A34", "14E4E6E172EA"), "3" -> ("388345A236BE", "5C63BFD90AE2"), "4" -> ("14E4E6E17648", "14E4E6E176C8"), "5" -> ("14E4E6E186A4", "EC172FE3B340"), "6" -> ("14E4E6E17908", "14E4E6E179F2"), "7" -> ("14E4E6E17950", "14E4E6E18658"), "8" -> ("14E4E6E1790A", "14E4E6E173FE"), "9" -> ("085700412D4E", "085700411A86"), "10" -> ("0C8268F15CB2", "0C8268F17FB8"), "19" -> ("0C8268C7E138", "0C8268EE3868"), "11" -> ("0C8268C804F8", ""), "12" -> ("0C8268EE3878", "0C8268EE7164"), "13" -> ("0C8268C7D518", "0C8268F17F60"), "14" -> ("0C8268EE3F32", "0857004127E2"), "15" -> ("0C8268F933A2", "0C8268F1648E"), "16" -> ("0C8268F90E64", "0C8268C7D504", "14E6E4E1C510", "0C8268C7DD6C"), "17" -> ("0C8268EE38EE", "0C8268F93B0A"), "18" -> ("0C8268F15C64", "0C8268F9314E"))
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost/wibupt"

  /**
    *
    * @param dataPath HDFS数据的路径
    * @param days     时间长度。读取最近几天的数据。eg:1
    * @param locStr   监测场所
    * @return 路径字符串
    */
  def readData(dataPath: StringBuilder, days: Int = 1, locStr: String = ""): String = {
    // 路径字符串
    val multiPaths = new StringBuilder
    // 存储相应日期的文件路径
    val arr_0 = new ArrayBuffer[String]()
    // 存储日期字符串
    val arr_2 = new ArrayBuffer[String]()
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    // locStr为空时,获取所有AP的数据
    if (locStr.equals("")) {
      for (i <- (0 until (days))) {
        val tmpPath = new StringBuilder
        tmpPath.append(dataPath).append("*/").append(sdf.format(cal.getTime))
        arr_0 += tmpPath.toString
      }
      multiPaths.append(arr_0.mkString(","))
      return multiPaths.toString()
    }
    // 获取locStr对应AP的数据
    else {
      var arr_1 = locMacMap.getOrElse(locStr, ()).toString.split("[^-\\w]+")
      // locStr合法,能获取到对应AP
      if (!arr_1.isEmpty) {
        arr_1 = arr_1.tail
        for (i <- (0 until (days))) {
          arr_2 += sdf.format(cal.getTime)
        }
        for (item <- arr_1; i <- (0 until (arr_2.length))) {
          val tmpPath = new StringBuilder
          tmpPath.append(dataPath).append(item).append("/").append(arr_2(i))
          arr_0 += tmpPath.toString
        }
        multiPaths.append(arr_0.mkString(","))
        return multiPaths.toString()
      }
      // 非法locStr返回空字符串
      else return ""
    }
  }

  /**
    *
    * @return 当天日期，形如"2017-01-01"
    */
  def getToday(): String = {
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    return sdf.format(cal.getTime)
  }

  /**
    *
    * @return 昨天日期
    */
  def getYesterday(): String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    return sdf.format(cal.getTime)
  }

  /**
    *
    * @param date 当天日期，形如"2017-01-01"
    * @return 时间戳，形如"1492444800"
    */
  def getgelin(date: String): Int = {
    var monTime = 0
    var tmp = null
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    try {
      monTime = String.valueOf(sdf.parse(date).getTime).substring(0, 10).toInt
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    return monTime
  }

  /**
    *
    * @param path 待删除目录的路径
    */
  def dirDel(path: File) {
    if (!path.exists())
      return
    else if (path.isFile()) {
      path.delete()
      //      println(path + ":  文件被删除")
      return
    }

    val file: Array[File] = path.listFiles()
    for (d <- file) {
      dirDel(d)
    }

    path.delete()
    //    println(path + ":  目录被删除")

  }

  /**
    *
    * @param s 待过滤字符串
    * @param ch 需要去除的字符
    * @return 过滤后的字符串
    */
  def stripChars(s: String, ch: String): String = {
    val str = s.filterNot(ch.contains(_))
    return str
  }
//  def stripChars(s:String, ch:String)= s filterNot (ch contains _)
  def str2arr(str: String, headStr: String, tailStr: String, splitStr: String) = {
    stripChars(stripChars(str, headStr), tailStr).split(splitStr)
  }

  /**
    *
    * @param rdd Spark RDD
    * @return 整理成数组
    */
  def rdd2arr(rdd: RDD[String]): Array[String] = {
    val arr = rdd.map(line=>{
      val line0 = stripChars(stripChars(line, "["), "]").split(",")(0)
      line0.asInstanceOf[String]
    }).collect()
    return arr
//    var newArr = new Array[String](arr.length)
//    newArr = arr
//    return newArr
  }
  /**
    *
    * @param user    mysql用户名
    * @param pwd     mysql密码
    * @param groupid 监测场所
    * @param count   实时流量
    */
  def insertTable(user: String, pwd: String, groupid: Int, count: Long): Unit = {
    var conn: Connection = null
    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, pwd)
      val sql = "INSERT INTO realtime_statistic(groupid, statistic, monTime) VALUES(?, ?, ?)"
      val pstmt = conn.prepareStatement(sql)
      pstmt.setInt(1, groupid)
      pstmt.setLong(2, count)
//      pstmt.setLong(3, getgelin(getToday()))
      pstmt.setLong(3, System.currentTimeMillis().toString.substring(0, 10).toInt)
      pstmt.execute()
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      if (conn != null) {
        conn.close()
      }
    }
  }
  def main(args: Array[String]): Unit = {

  }
}
