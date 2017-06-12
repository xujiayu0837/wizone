package org.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.stat.test.{BinarySample, StreamingTest}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by xujiayu on 17/5/23.
  */
object MllibStreamingDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MllibStreamingDemo").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(20))
    Logger.getRootLogger.setLevel(Level.WARN)
    ssc.checkpoint("/tmp/checkpoint")
    val Array(zkQuorum, groupId, topicStr, numThreads) = args
    val topics = topicStr.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)

//    val Array(zkQuorum, groupId, topicStr) = args
//    val topics = topicStr.toMap
//    val data = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics, StorageLevel.MEMORY_AND_DISK_SER)

//    val data = ssc.textFileStream("/Users/xujiayu/python").map(line=>line.split(",") match {
//      case Array(label, value) => BinarySample(label.toBoolean, value.toDouble)
//    })
//    val streamingTest = new StreamingTest().setPeacePeriod(0).setWindowSize(0).setTestMethod("welch").registerStream(data)
//    streamingTest.print()
//    var cnt = 10
//    streamingTest.foreachRDD{rdd=>
//      cnt -= 1
//      val anySignificant = rdd.map(_.pValue < 0.05).fold(false)(_ || _)
//      if (cnt == 0 || anySignificant) rdd.context.stop()
//    }

    ssc.start()
    ssc.awaitTermination()
  }
}
