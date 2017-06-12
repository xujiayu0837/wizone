package org.spark

import java.sql.Timestamp
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, lit, mean, round, explode, when}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable

/**
  * Created by xujiayu on 17/6/6.
  */
object StreamingDfDemo {
  val OUIFILENAME = new StringBuilder("/Users/xujiayu/Downloads/oui_new.txt")

  def stateSpecWordCount(key: String, value: Option[Int], state: State[Int]) = {
    val res = state.getOption().getOrElse(0) + value.getOrElse(0)
    val getState = state.getOption().getOrElse(0)
    val getValue = value.getOrElse(0)
    println(s"getState: $getState")
    println(s"getValue: $getValue")
    state.update(res)
    (key, res)
  }
//  def stateSpec(key: String, value: Option[MyUtils.dataWithId], state: State[Seq[MyUtils.dataWithId]]) = {
//    val getState = state.getOption().getOrElse(Seq[MyUtils.dataWithId]())
//    val getValue = value.get
//    val res = getState :+ getValue
//    println(s"getState: ${getState}")
//    println(s"getValue: ${getValue}")
//    state.update(res)
//    Some(res)
//  }
  def stateSpec(key: String, value: Option[MyUtils.dataWithTs], state: State[Tuple2[Int, Seq[MyUtils.dataWithTs]]]) = {
    val getState = state.getOption().getOrElse(Tuple2[Int, Seq[MyUtils.dataWithTs]](0, Seq[MyUtils.dataWithTs]()))
    val getValue = value.get
    val sum = getState._1 + 1
    val seq = getState._2 :+ getValue
    println(s"getState1: ${getState._1}")
    println(s"getState2: ${getState._2}")
    println(s"getValue: ${getValue}")
    state.update(Tuple2[Int, Seq[MyUtils.dataWithTs]](sum, seq))
    Some(seq)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingDfDemo").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(20))
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    ssc.checkpoint("/tmp/checkpoint")
    val Array(brokers, topics) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"->brokers
//      "auto.offset.reset" -> "smallest"
    )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2).map(_.split("[|]"))
//    val dataStream = lines.map(item=>data(item(0).trim, item(1).toDouble, new Timestamp(item(2).toLong * 1000L), item(3).trim))
    val dataStream = lines.map(item=>MyUtils.dataWithTs(item(0).trim, item(1).toDouble, item(2).toLong, item(3).trim))
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val ouiDs = spark.read.option("sep", "|").csv(OUIFILENAME.toString()).map(_.getString(0))
    val broadcastDs = broadcast(ouiDs)
//    println(s"ouiDs: ${ouiDs.collect().toBuffer}")
//    dataStream.foreachRDD{rdd=>
//      val df = rdd.toDF()
//      val filterDf = df.filter($"rssi".gt(-90)).filter($"AP".isin("14E4E6E186A4", "EC172FE3B340")).filter($"userMacAddr".substr(0, 6).isin(broadcastDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).orderBy($"userMacAddr", $"AP", $"ts").cache()
//      val zipDf = filterDf.rdd.zipWithIndex().map(tup=>MyUtils.dataWithId(tup._2, tup._1.getAs[String]("userMacAddr"), tup._1.getAs[Double]("rssi"), new Timestamp(tup._1.getAs[Long]("ts") * 1000L), tup._1.getAs[String]("AP"))).toDF()
//      zipDf.show(false)
//      val indexer = new StringIndexer().setInputCol("AP").setOutputCol("Indexer")
////      indexer.fit(filterDf).transform(filterDf).show(false)
//      zipDf.createOrReplaceTempView("data0")
//      zipDf.createOrReplaceTempView("data1")
//      zipDf.createOrReplaceTempView("data2")
//      spark.sql("SELECT COUNT(DISTINCT(userMacAddr)) FROM data0").show(2000, false)
//      val resDf = spark.sql("SELECT data1.userMacAddr, data1.ts, data1.AP FROM data1, data0, data2 WHERE data1.userMacAddr = data0.userMacAddr AND data1.AP = data0.AP AND data1._id - data0._id = 1 AND data1.rssi > data0.rssi AND data1.userMacAddr = data2.userMacAddr AND data1.AP = data2.AP AND data2._id - data1._id = 1 AND data1.rssi > data2.rssi").orderBy($"userMacAddr", $"ts").coalesce(4)
//      println(s"getNumPartitions: ${resDf.rdd.getNumPartitions}")
////      val resDf = df.withColumn("AP", lit(5)).withColumn("statistic", lit(-1)).withColumn("monTime", lit(1495597823)).drop("userMacAddr", "rssi", "ts", "locStr")
////      resDf.write.mode("append").jdbc("jdbc:mysql://localhost:3306/wibupt", "realtime_statistic", prop)
//      resDf.show(2000, false)
//    }

    val pairDstream = dataStream.map(item=>item.userMacAddr->item).mapWithState(StateSpec.function(stateSpec _))
//    val pairDstream = dataStream.transform(rdd=>rdd.zipWithIndex().map(tup=>MyUtils.dataWithId(tup._2, tup._1.userMacAddr, tup._1.rssi, new Timestamp(tup._1.ts * 1000L), tup._1.AP))).map(item=>item->item).mapWithState(StateSpec.function(stateSpec _))
//    pairDstream.print()
    pairDstream.foreachRDD{rdd=>
      val dataDf = rdd.toDF().select(explode($"value").as("coll")).select($"coll.*")
      val groupDf = MyUtils.addColGroupid(dataDf)
      groupDf.createOrReplaceTempView("data")
//      spark.sql("SELECT groupid, COUNT(groupid) FROM data GROUP BY groupid").show(false)
//      groupDf.orderBy($"userMacAddr", $"AP", $"ts").show(2000, false)
//      val filterDf = groupDf.filter($"rssi".gt(-90)).filter($"userMacAddr".substr(0, 6).isin(broadcastDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).orderBy($"userMacAddr", $"AP", $"ts").cache()
//      filterDf.show(2000, false)
//      val zipDf = filterDf.rdd.zipWithIndex().map(tup=>MyUtils.dataWithId(tup._2, tup._1.getAs[String]("userMacAddr"), tup._1.getAs[Double]("rssi"), new Timestamp(tup._1.getAs[Long]("ts") * 1000L), tup._1.getAs[String]("AP"))).toDF()
//      zipDf.show(2000, false)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}