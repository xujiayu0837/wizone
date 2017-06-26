package org.spark

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by xujiayu on 17/6/23.
  */
object TextClassification {
  val PATH = new StringBuilder("hdfs://10.103.93.27:9000/crawler_data/collect_train/*")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TextClassification").master("local[*]").getOrCreate()
    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.ERROR)

//    val df = spark.sparkContext.textFile(PATH.toString()).toDF()
//    df.map(_.mkString).map{item=>
//      ToAnalysis.parse(item)
//    }.show(false)

//    val rdd = spark.sparkContext.parallelize(Seq("欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!"))
    val stopWords = new StopRecognition
//    stopWords.insertStopNatures("null")
    stopWords.insertStopWords("library/stop.dic")
    val rdd = spark.sparkContext.textFile(PATH.toString())
    rdd.map{item=>
      val res = ToAnalysis.parse(item).recognition(stopWords)
      println(s"res: $res")
    }.count()
  }
}
