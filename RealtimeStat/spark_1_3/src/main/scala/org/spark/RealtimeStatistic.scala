package org.spark

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, explode, mean, round}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._

import scala.collection.mutable.StringBuilder

/**
  * Created by xujiayu on 17/6/15.
  */
object RealtimeStatistic {
  val THRESHOLD = 9273
  val durations = 1
  val numCores = 4
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")
  val OUIFILENAME = new StringBuilder("/Users/xujiayu/Downloads/oui_new.txt")
  //  val PARQUETPATH = new StringBuilder("/home/hadoop/parquet")
  //  val OUIFILENAME = new StringBuilder("/home/hadoop/oui_new.txt")

  def stateSpec(key: String, value: Option[MyUtils.dataWithId], state: State[Seq[MyUtils.dataWithId]]) = {
    val getState = state.getOption().getOrElse(Seq[MyUtils.dataWithId]())
    val getValue = value.get
    val res = getState :+ getValue
    //    println(s"getState: $getState")
    //    println(s"getValue: $getValue")
    state.update(res)
    Some(res)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RealtimeStatistic").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Minutes(durations))
//    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getRootLogger.setLevel(Level.ERROR)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    ssc.checkpoint("/tmp/checkpoint")
    val Array(brokers, topics, jdbcMysql, mysqlUser, mysqlPasswd) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"->brokers
      //      "auto.offset.reset" -> "smallest"
    )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2).map(_.split("[|]"))
    //    val dataStream = lines.map(item=>data(item(0).trim, item(1).toDouble, new Timestamp(item(2).toLong * 1000L), item(3).trim))
    val dataStream = lines.map(item=>MyUtils.dataWithId(item(0).toLong, item(1).trim, item(2).toDouble, item(3).toLong, item(4).trim))
    val prop = new Properties()
    prop.put("user", mysqlUser)
    prop.put("password", mysqlPasswd)
    val ouiDs = spark.read.option("sep", "|").csv(OUIFILENAME.toString()).map(_.getString(0))
    val broadcastDs = broadcast(ouiDs)
    val pairDstream = dataStream.map(item=>item.AP->item).mapWithState(StateSpec.function(stateSpec _).timeout(Seconds(9273)))
    //    val pairDstream = dataStream.transform(rdd=>rdd.zipWithIndex().map(tup=>MyUtils.dataWithDt(tup._2, tup._1.userMacAddr, tup._1.rssi, new Timestamp(tup._1.ts * 1000L), tup._1.AP))).map(item=>item->item).mapWithState(StateSpec.function(stateSpec _))
    //    pairDstream.print()
    dataStream.foreachRDD{rdd=>
      val dataDs = rdd.toDF().dropDuplicates().coalesce(numCores)
      val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).coalesce(numCores)
      //      dataDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).filter($"userMacAddr".substr(0, 6).isin(broadcastDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).coalesce(numCores).createOrReplaceTempView("data30")
      dataDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).coalesce(numCores).createOrReplaceTempView("data30")
      val sql0 = "SELECT data30.userMacAddr, data30.rssi, data30.ts, data30.AP FROM data30, (SELECT userMacAddr, ts, MAX(rssi) max FROM data30 GROUP BY userMacAddr, ts) data31 WHERE data30.userMacAddr = data31.userMacAddr AND data30.ts = data31.ts AND data30.rssi = data31.max ORDER BY data30.userMacAddr, data30.AP, data30.ts"
      val filterDs = spark.sql(sql0).coalesce(numCores)
      val trajWindow = Window.partitionBy("userMacAddr", "AP").orderBy("userMacAddr", "AP", "ts")
      val prevNextDf = MyUtils.addColsPrevNext(filterDs, trajWindow)
      val groupDf = MyUtils.addColGroupid(prevNextDf)
      val modifyDf = MyUtils.modifyColAP(groupDf)
      modifyDf.createOrReplaceTempView("data0")
      // 获取轨迹序列
      val trajSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data0 WHERE rssi > rssiPrev AND rssi > rssiNext"
      val trajDs = spark.sql(trajSql).orderBy($"groupid", $"userMacAddr", $"ts").coalesce(numCores)
      // 获取进出大楼人数
      val window = Window.partitionBy("groupid", "userMacAddr").orderBy("groupid", "userMacAddr", "ts")
      MyUtils.addColsPrev(trajDs, window).createOrReplaceTempView("data10")
      val comeSql = "SELECT userMacAddr, tsPrev, APPrev, rssiPrev, ts, AP, rssi, groupid FROM data10 WHERE AP = 'inside' AND APPrev = 'outside'"
      val goSql = "SELECT userMacAddr, tsPrev, APPrev, rssiPrev, ts, AP, rssi, groupid FROM data10 WHERE AP = 'outside' AND APPrev = 'inside'"
      spark.sql(comeSql).createOrReplaceTempView("comeData")
      spark.sql(goSql).createOrReplaceTempView("goData")
      val sql1 = "SELECT groupid, COUNT(groupid) AS comeCount FROM comeData GROUP BY groupid"
      val sql2 = "SELECT groupid, COUNT(groupid) AS goCount FROM goData GROUP BY groupid"
      val comeCount = spark.sql(sql1).coalesce(numCores)
      val goCount = spark.sql(sql2).coalesce(numCores)
      val comeGoDs = comeCount.join(goCount, Seq("groupid"), "fullouter").coalesce(numCores)
      val newJoinDf = MyUtils.modifyColCount(comeGoDs)
      newJoinDf.createOrReplaceTempView("count")
      val count = spark.sql("SELECT groupid, comeCount - goCount AS count FROM count")
      val tb0 = "(SELECT realtime_statistic.groupid, realtime_statistic.statistic AS base FROM realtime_statistic, (SELECT groupid, MAX(monTime) max FROM realtime_statistic GROUP BY groupid) data20 WHERE realtime_statistic.monTime = data20.max AND realtime_statistic.groupid = data20.groupid ORDER BY realtime_statistic.groupid) data21"
      val base = spark.read.jdbc(jdbcMysql, tb0, prop)
      val joinDs = base.join(count, Seq("groupid"), "fullouter").coalesce(numCores)
      MyUtils.modifyColBase(joinDs).createOrReplaceTempView("res")
      val resDf = MyUtils.addColMonTime(spark.sql("SELECT groupid, base + count AS statistic FROM res"))
      resDf.show(2000, false)
//      resDf.write.mode("append").jdbc(new StringBuilder(jdbcMysql).append("?verifyServerCertificate=false&useSSL=true").toString(), "realtime_statistic", prop)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
