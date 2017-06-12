package org.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{sum, when, lit, mean, stddev, stddev_pop}

/**
  * Created by xujiayu on 17/5/16.
  */
object mlDfDemo {
  val FILENAME = "/Users/xujiayu/python/20170220"
  val FILENAME0 = "hdfs://10.103.93.27:9000/scandata/14E4E6E186A4/2016*"
  val FILENAME1 = "hdfs://10.103.93.27:9000/scandata/EC172FE3B340/2016*"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("mlDfDemo").master("local[*]").config(new SparkConf()).getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import spark.implicits._

//    val df0 = spark.read.format("libsvm").load("/usr/local/spark-2.1.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt").persist()
//    println("Schema from LIBSVM:")
//    df0.printSchema()
//    println(s"Loaded training data as a DataFrame with ${df0.count()} records.")
//    val labelDf = df0.describe("label")
//    labelDf.show()
//    val featureRdd = df0.select("features").rdd.map{case Row(item: Vector) => item}
//    val featureSummary = featureRdd.aggregate(new MultivariateOnlineSummarizer())(
//      (summary, feat) => summary.add(Vectors.fromML(feat)),
//      (sum1, sum2) => sum1.merge(sum2)
//    )
//    println(s"Selected features column with average values:\n ${featureSummary.mean.toString}")
    val schema = StructType(
      List(
        StructField("userMacAddr", StringType, true),
        StructField("rssi", DoubleType, true),
        StructField("ts", StringType, true)
      )
    )
    val df1 = spark.read.option("sep", "|").schema(schema).csv(FILENAME).persist()
//    val groupid = FILENAME0.split("/")(4)
//    val df5 = df1.withColumn("groupid", lit(groupid))
//    df5.show(false)
    val rdd2 = df1.rdd.map{case Row(userMacAddr: String, rssi: Double, ts: String)=>((userMacAddr, ts), rssi)}.reduceByKey(_+_).sortBy(_._1._1)
    println(s"rdd2: ${rdd2.collect().toBuffer}")
    df1.groupBy($"userMacAddr").agg(stddev_pop($"rssi")).orderBy($"userMacAddr").show(2000, false)
    df1.groupBy($"userMacAddr", $"ts").agg(mean($"rssi")).orderBy($"userMacAddr", $"ts").show(2000, false)
//    df1.show(false)
//    println(s"cnt: ${df1.count()}")

//    val rdd0 = spark.sparkContext.parallelize(Seq(
//      (1, "cat1", 1), (1, "cat2", 3), (1, "cat9", 5), (2, "cat4", 6),
//      (2, "cat9", 2), (2, "cat10", 1), (3, "cat1", 5), (3, "cat7", 16),
//      (3, "cat8", 2)
//    )).persist()
//    val rdd1 = rdd0.groupBy(_._1).map{
//      case (id, rows)=>id -> rows
//          .map{
//            row=>row._2 -> row._3
//          }.toMap
//    }
//    val broadcast0 = spark.sparkContext.broadcast(rdd1.map(_._2.keySet).reduce(_.union(_)).toArray.sorted)
//    println(s"broadcast0: ${broadcast0.value.toBuffer}")
//    val broadcast1 = spark.sparkContext.broadcast(1 to 10 map (item=>s"cat$item") toArray)
//    println(s"broadcast1: ${broadcast1.value.toBuffer}")
//    val df4 = rdd1.map{item=>
//      item._1 -> broadcast0.value.map(item._2.getOrElse(_, 0))
//    }.toDF("userID", "features")
//    println(s"df4: ${df4.collect().toBuffer}")
//    val df2 = rdd0.toDF("userID", "category", "frequency")
//    val df3 = df2.select($"category").distinct().map(_.getString(0)).collect().sorted
//    println(s"df3: ${df3.toBuffer}")
//    val assembler = new VectorAssembler().setInputCols(df3).setOutputCol("features")
//    val arr0 = df3.map(item=>sum(when($"category" === item, $"frequency").otherwise(lit(0))).alias(item))
//    println(s"arr0: ${arr0.toBuffer}")
//    val transformed = assembler.transform(df2.groupBy($"userID").agg(arr0.head, arr0.tail: _*))//.select($"userID", $"features")
//    transformed.show(false)

    spark.stop()
  }
}
