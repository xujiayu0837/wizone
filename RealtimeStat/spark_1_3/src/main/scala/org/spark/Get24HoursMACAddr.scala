package org.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{dayofmonth, input_file_name}

/**
  * Created by xujiayu on 17/6/14.
  */
object Get24HoursMACAddr {
  val PATH = new StringBuilder("hdfs://10.103.93.27:9000/scandata/*/")
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")
  val DAYDELTA = -10
  val DATEFORMAT = "yyyyMMdd"
  val DAYFORMAT = "dd"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Get24HoursMACAddr").master("local[*]").getOrCreate()
    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.WARN)

    val cal = Calendar.getInstance()
    val dateFmt = new SimpleDateFormat(DATEFORMAT)
    val dayFmt = new SimpleDateFormat(DAYFORMAT)
    cal.add(Calendar.DATE, DAYDELTA)
    val time = cal.getTime
    val date = dateFmt.format(time)
    println(s"date: $date")
    val day = dayFmt.format(time)

    val dataframe = spark.read.option("sep", "|").schema(MyUtils.schema).csv(PATH.append(date).toString()).withColumn("AP", MyUtils.getAP(input_file_name))
//    dataframe.show(false)
    val datetimeDf = MyUtils.convertTimestampToDatetime(dataframe)
//    datetimeDf.show(false)
    val filterDf = datetimeDf.filter(dayofmonth($"ts").equalTo(day))
//    filterDf.show(false)
    val hourDf = MyUtils.addColHour(filterDf)
    hourDf.dropDuplicates("userMacAddr", "AP", "hour").createOrReplaceTempView("data")
    spark.sql("SELECT userMacAddr, AP, COUNT(*) AS count FROM data GROUP BY userMacAddr, AP").createOrReplaceTempView("data1")
    val resDf = spark.sql("SELECT userMacAddr FROM data1 WHERE count >= 24 ORDER BY userMacAddr").dropDuplicates("userMacAddr")
    resDf.show(2000, false)
//    resDf.write.mode("append").parquet(PARQUETPATH.toString())
//    hourDf.dropDuplicates("userMacAddr", "AP", "hour").filter($"userMacAddr".equalTo("6409802FC9DA")).coalesce(4).orderBy("userMacAddr", "AP", "hour").show(2000, false)
  }
}
