package org.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{input_file_name, mean, round}

/**
  * Created by xujiayu on 17/6/18.
  */
object DataframeDemo {
  val PATH = new StringBuilder("hdfs://10.103.93.27:9000/scandata/*/")
  val DAYDELTA = -1
  val DEVICEID = "4C8BEF352A08"
//  val DEVICEID = "BCA92031D550"
  val THRESHOLD = 1800
  val DATEFORMAT = "yyyyMMdd"
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")
  val CSVPATH = new StringBuilder("/Users/xujiayu/python/csv/")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataframeDemo").master("local[*]").getOrCreate()
    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.ERROR)

    val cal = Calendar.getInstance()
    val dateFmt = new SimpleDateFormat(DATEFORMAT)
    cal.add(Calendar.DATE, DAYDELTA)
    val time = cal.getTime
    val date = dateFmt.format(time)

//    val dataframe = spark.read.option("sep", "|").csv(PATH.append(date).toString()).withColumn("AP", MyUtils.getAP(input_file_name))
//    dataframe.filter($"_c2".equalTo("1498991437F40E22686E8A")).show(false)
    val dataframe = spark.read.option("sep", "|").option("mode", "DROPMALFORMED").schema(MyUtils.schema).csv(PATH.append(date).toString()).withColumn("AP", MyUtils.getAP(input_file_name))
//    println(s"dataframe getNumPartitions: ${dataframe.rdd.getNumPartitions}")
    val filterDs = dataframe.filter($"userMacAddr".equalTo(DEVICEID))
//    filterDs.show(2000, false)
//    println(s"filterDs getNumPartitions: ${filterDs.rdd.getNumPartitions}")
    val meanDs = filterDs.filter($"rssi".gt(-90)).groupBy("userMacAddr", "ts", "AP").agg(round(mean("rssi")).alias("rssi")).orderBy("userMacAddr", "AP", "ts")
//    println(s"meanDs getNumPartitions: ${meanDs.rdd.getNumPartitions}")
//    filterDs.filter($"rssi".gt(-90)).groupBy("userMacAddr", "ts", "AP").agg(round(mean("rssi")).alias("rssi")).createOrReplaceTempView("data10")
//    val meanDs = spark.sql("SELECT data10.userMacAddr, data10.rssi, data10.ts, data10.AP FROM data10, (SELECT userMacAddr, ts, MAX(rssi) max FROM data10 GROUP BY userMacAddr, ts) data11 WHERE data10.userMacAddr = data11.userMacAddr AND data10.ts = data11.ts AND data10.rssi = data11.max ORDER BY data10.userMacAddr, data10.AP, data10.ts")
    val zipDf = meanDs.coalesce(4).rdd.zipWithIndex().map(tup=>MyUtils.dataWithId(tup._2, tup._1.getAs[String]("userMacAddr"), tup._1.getAs[Double]("rssi"), tup._1.getAs[Long]("ts"), tup._1.getAs[String]("AP"))).toDF()
    val groupDf = MyUtils.addColGroupid(zipDf)
    val modifyDf = MyUtils.modifyColAP(groupDf)
    val csvDs = MyUtils.convertTimestampToDatetime(modifyDf).orderBy("ts", "AP")
//    csvDs.select("rssi", "ts", "AP", "groupid").coalesce(1).write.csv(CSVPATH.append(date).toString())
    csvDs.show(2000, false)
    val APWindow = Window.partitionBy("userMacAddr", "ts", "rssi").orderBy("userMacAddr", "ts", "rssi", "AP")
    MyUtils.addColAPPrev(csvDs, APWindow).createOrReplaceTempView("data30")
    val APSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data30 WHERE apPrev IS NULL"
    val APDs = spark.sql(APSql)
    APDs.orderBy("ts", "AP").show(2000, false)
    val rssWindow = Window.partitionBy("userMacAddr", "ts").orderBy("userMacAddr", "ts", "rssi")
    MyUtils.addColRssNext(APDs, rssWindow).createOrReplaceTempView("data31")
    //      MyUtils.addColRssNext(APDs, rssWindow).orderBy("groupid", "userMacAddr", "ts").show(2000, false)
    val rssSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data31 WHERE rssNext IS NULL"
    val trajDs = spark.sql(rssSql)
    trajDs.orderBy("ts", "AP").show(2000, false)
    // 获取进出大楼人数
    val window = Window.partitionBy("groupid", "userMacAddr").orderBy("groupid", "userMacAddr", "ts")
    MyUtils.addColsPrev(trajDs, window).createOrReplaceTempView("data10")
    val comeSql = "SELECT userMacAddr, tsPrev, apPrev, rssiPrev, ts, AP, rssi, groupid FROM data10 WHERE AP = 'inside' AND apPrev = 'outside'"
    val goSql = "SELECT userMacAddr, tsPrev, apPrev, rssiPrev, ts, AP, rssi, groupid FROM data10 WHERE AP = 'outside' AND apPrev = 'inside'"
    spark.sql(comeSql).createOrReplaceTempView("comeData")
    spark.sql(goSql).createOrReplaceTempView("goData")
    val sql1 = "SELECT groupid, COUNT(groupid) AS comeCount FROM comeData GROUP BY groupid"
    val sql2 = "SELECT groupid, COUNT(groupid) AS goCount FROM goData GROUP BY groupid"
    val sql3 = "SELECT * FROM comeData"
    val sql4 = "SELECT * FROM goData"
    spark.sql(sql3).show(2000, false)
    spark.sql(sql4).show(2000, false)

////    MyUtils.convertTimestampToDatetime(modifyDf).orderBy("groupid", "AP", "ts").show(2000, false)
//    modifyDf.createOrReplaceTempView("data0")
//    modifyDf.createOrReplaceTempView("data1")
//    modifyDf.createOrReplaceTempView("data2")
////    val trajSql = "SELECT data1.userMacAddr, data1.ts, data1.AP, data1.groupid, data1.rssi FROM data1, data0, data2 WHERE data1.userMacAddr = data0.userMacAddr AND data1.AP = data0.AP AND data1._id - data0._id = 1 AND data1.rssi > data0.rssi AND data1.userMacAddr = data2.userMacAddr AND data1.AP = data2.AP AND data2._id - data1._id = 1 AND data1.rssi > data2.rssi"
//    val trajSql = s"SELECT data1.userMacAddr, data1.ts, data1.AP, data1.groupid, data1.rssi FROM data1, data0, data2 WHERE data1.userMacAddr = data0.userMacAddr AND data1.AP = data0.AP AND data1._id - data0._id = 1 AND data1.ts - data0.ts <= $THRESHOLD AND data1.rssi > data0.rssi AND data1.userMacAddr = data2.userMacAddr AND data1.AP = data2.AP AND data2._id - data1._id = 1 AND data2.ts - data1.ts <= $THRESHOLD AND data1.rssi > data2.rssi"
//    val trajDs = spark.sql(trajSql).coalesce(4)
//    trajDs.orderBy("ts", "groupid").show(2000, false)
////    println(s"trajDs getNumPartitions: ${trajDs.rdd.getNumPartitions}")
////    val resDs = MyUtils.convertTimestampToDatetime(trajDs).orderBy("ts", "groupid")
////    resDs.show(2000, false)
  }
}
