package org.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{dayofmonth, input_file_name}

import scala.collection.mutable
import util.control.Breaks._

/**
  * Created by xujiayu on 17/6/14.
  */
object Get24HoursMACAddr {
  val PATH = new StringBuilder("hdfs://10.103.93.27:9000/scandata/*/")
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")
  val DAYDELTA = -1
  val DATEFORMAT = "yyyyMMdd"
  val DAYFORMAT = "dd"
  var BLACKLISTBUF = mutable.Buffer[String]()

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName("Get24HoursMACAddr").master("local[*]").getOrCreate()
      import spark.implicits._
      Logger.getRootLogger.setLevel(Level.WARN)
      val startTime = System.currentTimeMillis()

      val cal = Calendar.getInstance()
      val dateFmt = new SimpleDateFormat(DATEFORMAT)
      val dayFmt = new SimpleDateFormat(DAYFORMAT)
      cal.add(Calendar.DATE, DAYDELTA)
      val time = cal.getTime
      val date = dateFmt.format(time)
      println(s"date: $date")
      val day = dayFmt.format(time)

      val dataset = spark.read.option("sep", "|").option("mode", "DROPMALFORMED").schema(MyUtils.schema).csv(PATH.append(date).toString()).withColumn("AP", MyUtils.getAP(input_file_name)).coalesce(4)
      //    dataset.show(false)
      val datetimeDf = MyUtils.convertTimestampToDatetime(dataset)
      //    datetimeDf.show(false)
      val filterDs = datetimeDf.filter(dayofmonth($"ts").equalTo(day)).coalesce(4)
      //    filterDs.show(false)
      val hourDf = MyUtils.addColHour(filterDs)
      hourDf.dropDuplicates("userMacAddr", "AP", "hour").coalesce(4).createOrReplaceTempView("data")
      spark.sql("SELECT userMacAddr, AP, COUNT(*) AS count FROM data GROUP BY userMacAddr, AP").coalesce(4).createOrReplaceTempView("data1")
      val resDs = spark.sql("SELECT userMacAddr FROM data1 WHERE count >= 24 ORDER BY userMacAddr").dropDuplicates("userMacAddr").coalesce(4).cache()
      resDs.show(2000, false)
      //    resDs.write.mode("append").parquet(PARQUETPATH.toString())
      //    hourDf.dropDuplicates("userMacAddr", "AP", "hour").filter($"userMacAddr".equalTo("6409802FC9DA")).coalesce(4).orderBy("userMacAddr", "AP", "hour").show(2000, false)
      try {
        val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).dropDuplicates().coalesce(4)
        BLACKLISTBUF = blacklistDs.collect().toBuffer
      } catch {
        case e: Exception => e.printStackTrace()
      }
      val strDs = resDs.map(_.mkString)
      for (item <- strDs.collect()) {
        breakable {
          if (BLACKLISTBUF.contains(item)) {
            break()
          }
          else {
            println(s"$item")
          }
        }
      }
      BLACKLISTBUF ++= strDs.collect().toBuffer
      val blacklistDf = BLACKLISTBUF.distinct.toDF()
//      blacklistDf.show(2000, false)
//      blacklistDf.write.mode("overwrite").parquet(PARQUETPATH.toString())
      println(s"${System.currentTimeMillis() - startTime}")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
