package org.spark

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.functions.{broadcast, expr, lit, mean, round}

import scala.concurrent.duration._

/**
  * Created by xujiayu on 17/5/26.
  */
object StructuredKafkaWordCount {
  val OUIFILENAME = new StringBuilder("/Users/xujiayu/Downloads/oui_new.txt")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf()).appName("StructuredKafkaWordCount").master("local[*]").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import spark.implicits._
    val dataDf = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.103.93.27:9093").option("subscribe", "scandata_1_2").load()
//    val dataDs = dataDf.selectExpr("CAST(value AS STRING)").as[String]
//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "root")
//    dataDs.write.mode("append").jdbc("jdbc:mysql://localhost:3306/wibupt", "realtime_statistic", prop)
//    val query = dataDs.flatMap(_.split("[|]")).groupBy("value").count().withColumn("groupid", lit(5)).withColumn("monTime", lit(1493709834)).drop("value").writeStream.outputMode("complete").format("console").trigger(ProcessingTime(20.seconds)).start()
    val ouiDs = spark.read.option("sep", "|").csv(OUIFILENAME.toString()).map(_.getString(0))
    val broadcastDs = broadcast(ouiDs)
    println(s"ouiDs: ${ouiDs.collect().toBuffer}")
    val dataDs = dataDf.selectExpr("CAST(value AS STRING)").as[String].select(expr("(split(value, '[|]'))[0]").cast("string").as("userMacAddr"), expr("(split(value, '[|]'))[1]").cast("int").as("rssi"), expr("(split(value, '[|]'))[2]").cast("int").as("ts"), expr("(split(value, '[|]'))[3]").cast("string").as("groupid")).filter($"rssi".gt(-90)).filter($"groupid".isin("14E4E6E186A4", "EC172FE3B340")).filter($"userMacAddr".substr(0, 6).isin(broadcastDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"groupid").agg(round(mean($"rssi")).alias("rssi")).orderBy($"userMacAddr", $"ts")
    dataDs.printSchema()
//    dataDs.mapPartitions((it)=>{it.map(item=>item)})
    val query = dataDs.writeStream.outputMode("complete").format("console").trigger(ProcessingTime(20.seconds)).start()
    query.awaitTermination()
  }
}
