package org.spark

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{mean, round}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * Created by xujiayu on 17/6/27.
  */
object StreamingTest {
  val durations = 5
//  val numCores = 4
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingDfDemo").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Minutes(durations))
    Logger.getRootLogger.setLevel(Level.ERROR)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    ssc.checkpoint("/tmp/checkpoint")
    val Array(brokers, topics) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"->brokers
    )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//    messages.map(_._2).map(_.split("[|]")).filter(_.length != 5).print()
    val lines = messages.map(_._2).map(_.split("[|]")).filter(_.length == 5)
    val dataStream = lines.map(item=>MyUtils.dataWithId(item(0).toLong, item(1).trim, item(2).toDouble, item(3).toLong, item(4).trim))
    dataStream.foreachRDD{rdd=>
      val dataDs = rdd.toDF().coalesce(4)
      val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).coalesce(4)
      val filterDs = dataDs.filter($"AP".isin(MyUtils.groupid16:_*)).coalesce(4)
      val meanDs = filterDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).coalesce(4)
      meanDs.dropDuplicates("userMacAddr", "ts", "AP").coalesce(4).createOrReplaceTempView("data")
      spark.sql("SELECT userMacAddr, ts, COUNT(*) AS count FROM data GROUP BY userMacAddr, ts").coalesce(4).createOrReplaceTempView("data1")
      val countDs = spark.sql("SELECT userMacAddr, ts, count FROM data1 WHERE count >= 3").coalesce(4)
      val resDs = MyUtils.convertTimestampToDatetime(countDs).orderBy("userMacAddr", "ts").coalesce(4)
      resDs.show(2000, false)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
