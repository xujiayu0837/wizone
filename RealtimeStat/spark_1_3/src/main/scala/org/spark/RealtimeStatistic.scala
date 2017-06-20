package org.spark

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, explode, mean, round}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._

/**
  * Created by xujiayu on 17/6/15.
  */
object RealtimeStatistic {
  val THRESHOLD = 9273
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
    val ssc = new StreamingContext(conf, Minutes(1))
//    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getRootLogger.setLevel(Level.ERROR)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    ssc.checkpoint("/tmp/checkpoint")
    val Array(brokers, topics, mysqlUser, mysqlPasswd) = args
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
    pairDstream.foreachRDD{rdd=>
      val dataDf = rdd.toDF().select(explode($"value").as("coll")).select($"coll.*").dropDuplicates()
      val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).dropDuplicates()
      val filterDf = dataDf.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).filter($"userMacAddr".substr(0, 6).isin(broadcastDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).orderBy($"userMacAddr", $"AP", $"ts")
      val zipDf = filterDf.rdd.zipWithIndex().map(tup=>MyUtils.dataWithId(tup._2, tup._1.getAs[String]("userMacAddr"), tup._1.getAs[Double]("rssi"), tup._1.getAs[Long]("ts"), tup._1.getAs[String]("AP"))).toDF()
      val groupDf = MyUtils.addColGroupid(zipDf)
      val modifyDf = MyUtils.modifyColAP(groupDf).cache()
      //      modifyDf.show(2000, false)
      modifyDf.createOrReplaceTempView("data0")
      modifyDf.createOrReplaceTempView("data1")
      modifyDf.createOrReplaceTempView("data2")
      // 获取轨迹序列
      val trajSql = "SELECT data1.userMacAddr, data1.ts, data1.AP, data1.groupid, data1.rssi FROM data1, data0, data2 WHERE data1.userMacAddr = data0.userMacAddr AND data1.AP = data0.AP AND data1._id - data0._id = 1 AND data1.rssi > data0.rssi AND data1.userMacAddr = data2.userMacAddr AND data1.AP = data2.AP AND data2._id - data1._id = 1 AND data1.rssi > data2.rssi"
      val trajDs = spark.sql(trajSql).orderBy($"groupid", $"userMacAddr", $"ts").coalesce(4)
      val zipTrajDf = trajDs.rdd.zipWithIndex().map(tup=>MyUtils.data(tup._2, tup._1.getAs[String]("userMacAddr"), tup._1.getAs[Double]("rssi"), tup._1.getAs[Long]("ts"), tup._1.getAs[String]("AP"), tup._1.getAs[Int]("groupid"))).toDF().cache()
      zipTrajDf.createOrReplaceTempView("data10")
      zipTrajDf.createOrReplaceTempView("data11")
      // 获取进出大楼人数
      val comeSql = s"SELECT data10.userMacAddr, data10.ts, data10.AP, data11.ts, data11.AP, data10.groupid FROM data10, data11 WHERE data10.userMacAddr = data11.userMacAddr AND data10.groupid = data11.groupid AND data11._id - data10._id = 1 AND data11.ts - data10.ts <= $THRESHOLD AND data10.AP = '0' AND data11.AP = '1'"
      val goSql = s"SELECT data10.userMacAddr, data10.ts, data10.AP, data11.ts, data11.AP, data10.groupid FROM data10, data11 WHERE data10.userMacAddr = data11.userMacAddr AND data10.groupid = data11.groupid AND data11._id - data10._id = 1 AND data11.ts - data10.ts <= $THRESHOLD AND data10.AP = '1' AND data11.AP = '0'"
      spark.sql(comeSql).createOrReplaceTempView("comeData")
      spark.sql(goSql).createOrReplaceTempView("goData")
      val sql1 = "SELECT groupid, COUNT(groupid) AS comeCount FROM comeData GROUP BY groupid"
      val sql2 = "SELECT groupid, COUNT(groupid) AS goCount FROM goData GROUP BY groupid"
      val comeCount = spark.sql(sql1)
      val goCount = spark.sql(sql2)
      val joinDf = comeCount.join(goCount, Seq("groupid"), "fullouter")
      //      joinDf.show(false)
      val newJoinDf = MyUtils.modifyCol(joinDf)
      newJoinDf.createOrReplaceTempView("res")
      val countDf = spark.sql("SELECT groupid, comeCount - goCount AS statistic FROM res")
      val resDf = MyUtils.addColMonTime(countDf)
      resDf.show(false)
//      resDf.write.mode("append").jdbc("jdbc:mysql://10.103.93.27:3306/test", "realtime_statistic", prop)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
