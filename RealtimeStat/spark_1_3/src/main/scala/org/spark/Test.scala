package org.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{input_file_name, mean, round}

/**
  * Created by xujiayu on 17/6/27.
  */
object Test {
  val PATH = new StringBuilder("hdfs://10.103.93.27:9000/scandata/*/")
  val DAYDELTA = -1
  val THRESHOLD = 1800
  val DATEFORMAT = "yyyyMMdd"
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataframeDemo").master("local[*]").getOrCreate()
    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.ERROR)

    val cal = Calendar.getInstance()
    val dateFmt = new SimpleDateFormat(DATEFORMAT)
    cal.add(Calendar.DATE, DAYDELTA)
    val time = cal.getTime
    val date = dateFmt.format(time)

    val dataframe = spark.read.option("sep", "|").schema(MyUtils.schema).csv(PATH.append(date).toString()).withColumn("AP", MyUtils.getAP(input_file_name)).coalesce(4)
    val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).coalesce(4)
    val filterDs = dataframe.filter($"AP".isin(MyUtils.groupid16:_*)).coalesce(4)
    val meanDs = filterDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).coalesce(4)
    meanDs.dropDuplicates("userMacAddr", "ts", "AP").coalesce(4).createOrReplaceTempView("data")
    spark.sql("SELECT userMacAddr, ts, COUNT(*) AS count FROM data GROUP BY userMacAddr, ts").coalesce(4).createOrReplaceTempView("data1")
    val countDs = spark.sql("SELECT userMacAddr, ts, count FROM data1 WHERE count >= 3").coalesce(4)
    val resDs = MyUtils.convertTimestampToDatetime(countDs).orderBy("userMacAddr", "ts").coalesce(4)
    resDs.show(2000, false)
  }
}
