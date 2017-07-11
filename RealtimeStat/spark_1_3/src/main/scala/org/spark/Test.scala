package org.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{dayofmonth, input_file_name, mean, round, window, count}

/**
  * Created by xujiayu on 17/6/27.
  */
object Test {
  val PATH = new StringBuilder("hdfs://10.103.93.27:9000/scandata/*/")
  val DAYDELTA = -1
  val durations = 5
  val numCores = 4
  val THRESHOLD = 1800
  val DATEFORMAT = "yyyyMMdd"
  val DAYFORMAT = "dd"
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataframeDemo").master("local[*]").getOrCreate()
    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.ERROR)

    val cal = Calendar.getInstance()
    val dateFmt = new SimpleDateFormat(DATEFORMAT)
    val dayFmt = new SimpleDateFormat(DAYFORMAT)
    cal.add(Calendar.DATE, DAYDELTA)
    val time = cal.getTime
    val date = dateFmt.format(time)
    val day = dayFmt.format(time)

    val dataset = spark.read.option("sep", "|").schema(MyUtils.schema).csv(PATH.append(date).toString()).withColumn("AP", MyUtils.getAP(input_file_name)).coalesce(numCores)
    val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).coalesce(numCores)
    val groupDf = MyUtils.addColGroupid(dataset)
    val datetimeDf = MyUtils.convertTimestampToDatetime(groupDf)
    val filterDs = datetimeDf.filter(dayofmonth($"ts").equalTo(day)).coalesce(numCores)
    val meanDs = filterDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).groupBy("userMacAddr", "ts", "AP", "groupid").agg(round(mean($"rssi")).alias("rssi")).orderBy("userMacAddr", "AP", "ts").coalesce(numCores)
    val trajWindow = Window.partitionBy("userMacAddr", "AP").orderBy("userMacAddr", "AP", "ts")
    val prevNextDf = MyUtils.addColsPrevNext(meanDs, trajWindow)
    val modifyDf = MyUtils.modifyColAP(prevNextDf)
//    modifyDf.cache()
//    modifyDf.orderBy("groupid", "userMacAddr", "ts").show(2000, false)
    modifyDf.createOrReplaceTempView("data0")
    val peakSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data0 WHERE (rssi > rssiPrev AND rssi > rssiNext) OR (rssi > rssiPrev AND rssiNext IS NULL) OR (rssiPrev IS NULL AND rssi > rssiNext)"
    val trajDs = spark.sql(peakSql).orderBy("groupid", "userMacAddr", "ts").coalesce(numCores)
//    trajDs.show(2000, false)
    // 获取进出大楼人数
    val window1 = Window.partitionBy("groupid", "userMacAddr").orderBy("groupid", "userMacAddr", "ts")
    MyUtils.addColsPrev(trajDs, window1).createOrReplaceTempView("data10")
    val comeSql = "SELECT userMacAddr, tsPrev, APPrev, rssiPrev, ts, AP, rssi, groupid FROM data10 WHERE AP = 'inside' AND APPrev = 'outside'"
    val goSql = "SELECT userMacAddr, tsPrev, APPrev, rssiPrev, ts, AP, rssi, groupid FROM data10 WHERE AP = 'outside' AND APPrev = 'inside'"
    val comeDf = spark.sql(comeSql).coalesce(numCores)
    val goDf = spark.sql(goSql).coalesce(numCores)
//    comeDf.cache()
//    comeDf.groupBy($"groupid", window($"ts", new StringBuilder(durations.toString).append(" minutes").toString())).agg(count("window")).orderBy("groupid", "window").coalesce(numCores).show(2000, false)
    val comeCount = comeDf.groupBy($"groupid", window($"ts", new StringBuilder(durations.toString).append(" minutes").toString())).agg(count("groupid").alias("comeCount")).coalesce(numCores)
    val goCount = goDf.groupBy($"groupid", window($"ts", new StringBuilder(durations.toString).append(" minutes").toString())).agg(count("groupid").alias("goCount")).coalesce(numCores)
//    comeCount.orderBy("groupid", "window").show(2000, false)
//    goCount.orderBy("groupid", "window").show(2000, false)
    val comeGoDs = comeCount.join(goCount, Seq("groupid", "window"), "fullouter").coalesce(numCores)
//    comeGoDs.orderBy("groupid", "window").show(2000, false)
    val newJoinDf = MyUtils.modifyColCount(comeGoDs)
    newJoinDf.createOrReplaceTempView("count")
    val countDf = spark.sql("SELECT groupid, window, comeCount - goCount AS count FROM count").orderBy("groupid", "window")
    countDf.show(2000, false)

    println(s"maxMemory: ${Runtime.getRuntime.maxMemory()}")
    println(s"totalMemory: ${Runtime.getRuntime.totalMemory()}")

//    val filterDs = dataset.filter($"AP".isin(MyUtils.groupid16:_*)).coalesce(numCores)
//    val meanDs = filterDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).groupBy("userMacAddr", "ts", "AP").agg(round(mean($"rssi")).alias("rssi")).orderBy("userMacAddr", "AP", "ts").coalesce(numCores)
//    meanDs.dropDuplicates("userMacAddr", "ts", "AP").coalesce(numCores).createOrReplaceTempView("data")
//    spark.sql("SELECT userMacAddr, ts, COUNT(*) AS count FROM data GROUP BY userMacAddr, ts").coalesce(numCores).createOrReplaceTempView("data1")
//    val countDs = spark.sql("SELECT userMacAddr, ts, count FROM data1 WHERE count >= 3").coalesce(numCores)
//    val resDs = MyUtils.convertTimestampToDatetime(countDs).orderBy("userMacAddr", "ts").coalesce(numCores)
//    resDs.show(2000, false)
  }
}
