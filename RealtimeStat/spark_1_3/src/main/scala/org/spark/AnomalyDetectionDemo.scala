package org.spark

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by xujiayu on 17/5/12.
  */
object AnomalyDetectionDemo {
  val FILENAME = "/Users/xujiayu/spark-anomaly-detection/src/test/resources/training.csv"
  val LABEL = "/Users/xujiayu/spark-anomaly-detection/src/test/resources/cross_val.csv"
  val THRESHOLD = 0.1

  def getProbability(x: Vector, mean: Vector, variance: Vector): Double = {
    val zippedList = (x.toArray, mean.toArray, variance.toArray).zipped.toList
    val product = zippedList.map(tup=>{
      val exp = -Math.pow((tup._1 - tup._2), 2) / (2 * tup._3)
      val probability = Math.pow(Math.E, exp) / Math.sqrt(2 * tup._3 * Math.PI)
      probability
    }).product
    product
  }
  def getCount(labelPredictions: RDD[(Double, Double)], trueOrFalse: Double, posOrNeg: Double): Double = {
    labelPredictions.filter(tup=>{
      tup._1 == trueOrFalse && tup._2 == posOrNeg
    }).count().toDouble
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf()).appName("AnomalyDetectionDemo").master("local[*]").getOrCreate()
    val vectorRdd = spark.sparkContext.textFile(FILENAME).map(line=>{
      val strArr = line.split(",")
      val arr = strArr.map(_.toDouble)
      Vectors.dense(arr)
    })
    val labelRdd = spark.sparkContext.textFile(LABEL).map(line=>{
      val strArr = line.split(",")
      val arr = strArr.map(_.toDouble)
      LabeledPoint(arr(0), Vectors.dense(arr.slice(1, arr.length)))
    })
    val stat = Statistics.colStats(vectorRdd)
    val probabilityRdd = labelRdd.map(labelPoint=>{
      getProbability(labelPoint.features, stat.mean, stat.variance)
    })
    val minProbability = probabilityRdd.min()
    val maxProbability = probabilityRdd.max()
    val step = (maxProbability - minProbability) / 1000
    for (THRESHOLD <- minProbability to maxProbability by step) {
      val predictionRdd = probabilityRdd.map(probability=>{
        if (probability < THRESHOLD)
          1.0
        else
          0.0
      })
      val zipRdd = labelRdd.map(_.label).zip(predictionRdd)
      val numTP = getCount(zipRdd, 1.0, 1.0)
      val numFN = getCount(zipRdd, 1.0, 0.0)
      val numFP = getCount(zipRdd, 0.0, 1.0)
    }

    spark.sparkContext.stop()
  }
}
