package org.apache.spark

import java.sql.{Connection, DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{countDistinct, last, window}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xujiayu on 17/4/10.
  */
object RealtimeTraffic {
  val locMacMap = mutable.Map("1" -> ("14E4E6E16E7A", "14E4E6E1867A"), "2" -> ("14E4E6E17A34", "14E4E6E172EA"), "3" -> ("388345A236BE", "5C63BFD90AE2"), "4" -> ("14E4E6E17648", "14E4E6E176C8"), "5" -> ("14E4E6E186A4", "EC172FE3B340"), "6" -> ("14E4E6E17908", "14E4E6E179F2"), "7" -> ("14E4E6E17950", "14E4E6E18658"), "8" -> ("14E4E6E1790A", "14E4E6E173FE"), "9" -> ("085700412D4E", "085700411A86"), "10" -> ("0C8268F15CB2", "0C8268F17FB8"), "19" -> ("0C8268C7E138", "0C8268EE3868"), "11" -> ("0C8268C804F8", ""), "12" -> ("0C8268EE3878", "0C8268EE7164"), "13" -> ("0C8268C7D518", "0C8268F17F60"), "14" -> ("0C8268EE3F32", "0857004127E2"), "15" -> ("0C8268F933A2", "0C8268F1648E"), "16" -> ("0C8268F90E64", "0C8268C7D504", "14E6E4E1C510", "0C8268C7DD6C"), "17" -> ("0C8268EE38EE", "0C8268F93B0A"), "18" -> ("0C8268F15C64", "0C8268F9314E"))
  // 数据的路径
  val DATAPATH = "hdfs://10.103.24.161:9000/scandata"
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost/wibupt"

  /**
    *
    * @param dataPath HDFS数据的路径
    * @param days     时间长度。读取最近几天的数据。eg:1
    * @param locStr   监测场所
    * @return 路径字符串
    */
  def readData(dataPath: String, days: Int = 1, locStr: String = ""): String = {
    // HDFS文件名
    var path_1 = ""
    // 路径字符串
    var multiPaths = ""
    // 存储相应日期的文件路径
    val arr_0 = new ArrayBuffer[String]()
    // 存储日期字符串
    val arr_2 = new ArrayBuffer[String]()
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    // locStr为空时,获取所有AP的数据
    if (locStr == "") {
      for (i <- (0 until (days))) {
        path_1 = if (dataPath.endsWith("/")) dataPath + "*/" else dataPath + "/*/"
        path_1 += sdf.format(cal.getTime)
        arr_0 += path_1
      }
      multiPaths = arr_0.mkString(",")
      return multiPaths
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
          if (!dataPath.endsWith("/")) path_1 = dataPath + "/"
          path_1 += item + "/" + arr_2(i)
          arr_0 += path_1
        }
        multiPaths = arr_0.mkString(",")
        return multiPaths
      }
      // 非法locStr返回空字符串
      else return ""
    }
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
      val sql = "INSERT INTO realtime_traffic(groupid, traffic, monTime) VALUES(?, ?, ?)"
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

  /**
    *
    * @param groupid 监测场所
    * @param args mysql用户名以及密码
    * @param spark spark环境
    */
  def run(groupid: Int, args: Array[String], spark: SparkSession): Unit = {
    try {
      import spark.implicits._
      // 读取数据
      val multiPaths = readData(DATAPATH, 1, groupid + "")
      val fc = classOf[TextInputFormat]
      val kc = classOf[LongWritable]
      val vc = classOf[Text]
      val hadoopConf = new Configuration()
      hadoopConf.set("defaultFS", "hdfs://10.103.24.161:9000")
      val originRdd = spark.sparkContext.newAPIHadoopFile(multiPaths, fc, kc, vc, hadoopConf)
      val hadoopRdd = originRdd.asInstanceOf[NewHadoopRDD[LongWritable, Text]].mapPartitionsWithInputSplit((inputSplit, it) => {
        val file = inputSplit.asInstanceOf[FileSplit]
        it.map(tup => {
          val content = tup._2
          val apMacAddr = file.getPath.toString.split('/')(4)
          val locStr = locMacMap.find(_._2.toString.contains(apMacAddr)).getOrElse(("", ()))._1
          content.toString+"|"+locStr
//          content.toString + "|" + apMacAddr
        })
      })
      val dataRdd = hadoopRdd.filter(_.split("[|]").length == 4).filter(line => {
        val arr = line.split("[|]")
        arr(1).toDouble > -90
      }).map(_.split("[|]"))
      val tmpRdd = dataRdd.map(arr => Row(arr(0).trim, arr(1).toDouble, new Timestamp(arr(2).toLong * 1000L), arr(3).trim))
      // 传入数据类型参数
      val schema = StructType(
        List(
          StructField("userMacAddr", StringType, true),
          StructField("rssi", DoubleType, true),
          StructField("ts", TimestampType, true),
          StructField("locStr", StringType, true)
        )
      )
      val dataDf = spark.createDataFrame(tmpRdd, schema)
      val dataDs = dataDf.as[data]
      // 把热数据缓存到内存中
      dataDs.persist()
      // 每5分钟为一个时间段,相同MAC地址只算一个用户。不同时间段分开统计
      val windowDs = dataDs.groupBy(window($"ts", "5 minutes").as("time")).agg(countDistinct("userMacAddr").as("count"))
      //      val resDs = windowDs.select("time.start", "time.end", "count").orderBy("start")
      val resDs = windowDs.sort("time.start").select("time.start", "time.end", "count")
      // 测试
      //      resDs.show(300, truncate = false)
      try {
        // 获取计算结果
        val cnt = resDs.select(last("count")).first().getLong(0)
        insertTable(args(0), args(1), groupid, cnt)
        // 测试
//        println("res: " + cnt)
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
      dataDs.unpersist()
    }
    catch {
      case e: Exception => e.printStackTrace()
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

  def main(args: Array[String]): Unit = {
    // 获取配置文件，并初始化spark
    // 测试
//    val spark = SparkSession.builder().config(new SparkConf()).appName("RealtimeTraffic").master("local[*]").getOrCreate()
    val spark = SparkSession.builder().config(new SparkConf()).appName("RealtimeTraffic").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    while (true) {
      try {
        for (i <- (1 until (20))) {
          run(i, args, spark)
        }
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
    }
    spark.sparkContext.stop()
  }
}

case class data(userMacAddr: String, rssi: Double, ts: Timestamp, locStr: String)