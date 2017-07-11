package org.spark

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, mean, round}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, State, StreamingContext}

import scala.collection.mutable.StringBuilder

/**
  * Created by xujiayu on 17/6/27.
  */
object StreamingTest {
  val durations = 5
  val numCores = 4
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")

  def stateSpecWordCount(key: String, value: Option[Int], state: State[Int]) = {
    val res = state.getOption().getOrElse(0) + value.getOrElse(0)
    val getState = state.getOption().getOrElse(0)
    val getValue = value.getOrElse(0)
    println(s"getState: $getState")
    println(s"getValue: $getValue")
    state.update(res)
    (key, res)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingDfDemo").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Minutes(durations))
    Logger.getRootLogger.setLevel(Level.ERROR)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    ssc.checkpoint("/tmp/checkpoint")
    val Array(brokers, topics, jdbcMysql, mysqlUser, mysqlPasswd) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"->brokers
    )
    val prop = new Properties()
    prop.put("user", mysqlUser)
    prop.put("password", mysqlPasswd)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//    messages.map(_._2).map(_.split("[|]")).filter(_.length != 5).print()
    val lines = messages.map(_._2).map(_.split("[|]")).filter(_.length == 5)
    val dataStream = lines.map(item=>MyUtils.dataWithId(item(0).toLong, item(1).trim, item(2).toDouble, item(3).toLong, item(4).trim))
    val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).coalesce(numCores).persist(StorageLevel.MEMORY_AND_DISK_SER)
    dataStream.foreachRDD{rdd=>
      val dataDs = rdd.toDF().repartition(numCores).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val filterDs = dataDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).orderBy($"userMacAddr", $"AP", $"ts").coalesce(numCores)
      // 获取轨迹序列模块
      val peakWindow = Window.partitionBy("userMacAddr", "AP").orderBy("userMacAddr", "AP", "ts")
      val prevNextDf = MyUtils.addColsPrevNext(filterDs, peakWindow)
      val groupDf = MyUtils.addColGroupid(prevNextDf)
      val modifyDf = MyUtils.modifyColAP(groupDf)
      modifyDf.createOrReplaceTempView("data0")
      //      val countSql = "SELECT data0.userMacAddr, data0.ts, data0.rssi, data0.AP, data0.groupid, data40.count FROM data0, (SELECT userMacAddr, ts, rssi, COUNT(*) AS count FROM data0 GROUP BY userMacAddr, ts, rssi) data40 WHERE data0.userMacAddr = data40.userMacAddr AND data0.ts = data40.ts AND data0.rssi = data40.rssi AND data40.count > 1"
      //      spark.sql(countSql).orderBy("userMacAddr", "ts", "rssi").show(2000, false)
      val peakSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data0 WHERE (rssi > rssiPrev AND rssi > rssiNext) OR (rssi > rssiPrev AND rssiNext IS NULL) OR (rssiPrev IS NULL AND rssi > rssiNext)"
      val peakDs = spark.sql(peakSql).coalesce(numCores)
      //      peakDs.orderBy("groupid", "userMacAddr", "ts").show(2000, false)
      val APWindow = Window.partitionBy("userMacAddr", "ts", "rssi").orderBy("userMacAddr", "ts", "rssi", "AP")
      MyUtils.addColAPPrev(peakDs, APWindow).createOrReplaceTempView("data30")
      val APSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data30 WHERE apPrev IS NULL"
      val APDs = spark.sql(APSql).coalesce(numCores)
      val rssWindow = Window.partitionBy("userMacAddr", "ts").orderBy("userMacAddr", "ts", "rssi")
      MyUtils.addColRssNext(APDs, rssWindow).createOrReplaceTempView("data31")
      //      MyUtils.addColRssNext(APDs, rssWindow).orderBy("groupid", "userMacAddr", "ts").show(2000, false)
      val rssSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data31 WHERE rssNext IS NULL"
      val trajDs = spark.sql(rssSql).coalesce(numCores)
      trajDs.withColumn("groupid", lit(0)).withColumn("statistic", lit(-1)).createOrReplaceTempView("res")
      val resDf = MyUtils.addColMonTime(spark.sql("SELECT groupid, statistic FROM res")).persist(StorageLevel.MEMORY_AND_DISK_SER)
      resDf.write.mode("append").jdbc(new StringBuilder(jdbcMysql).append("?verifyServerCertificate=false&useSSL=true").toString(), "realtime_statistic_streaming_test", prop)
    }
//    dataStream.foreachRDD{rdd=>
//      val dataDs = rdd.toDF().coalesce(4)
//      val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).coalesce(4)
//      val filterDs = dataDs.filter($"AP".isin(MyUtils.groupid16:_*)).coalesce(4)
//      val meanDs = filterDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).coalesce(4)
//      meanDs.dropDuplicates("userMacAddr", "ts", "AP").coalesce(4).createOrReplaceTempView("data")
//      spark.sql("SELECT userMacAddr, ts, COUNT(*) AS count FROM data GROUP BY userMacAddr, ts").coalesce(4).createOrReplaceTempView("data1")
//      val countDs = spark.sql("SELECT userMacAddr, ts, count FROM data1 WHERE count >= 3").coalesce(4)
//      val resDs = MyUtils.convertTimestampToDatetime(countDs).orderBy("userMacAddr", "ts").coalesce(4)
//      resDs.show(2000, false)
//    }
    ssc.start()
    ssc.awaitTermination()
  }
}
