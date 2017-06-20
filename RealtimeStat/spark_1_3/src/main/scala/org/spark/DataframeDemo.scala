package org.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{input_file_name, round, mean}

/**
  * Created by xujiayu on 17/6/18.
  */
object DataframeDemo {
  val PATH = new StringBuilder("hdfs://10.103.93.27:9000/scandata/*/")
  val DAYDELTA = -1
  val DATEFORMAT = "yyyyMMdd"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataframeDemo").master("local[*]").getOrCreate()
    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.ERROR)

    val cal = Calendar.getInstance()
    val dateFmt = new SimpleDateFormat(DATEFORMAT)
    cal.add(Calendar.DATE, DAYDELTA)
    val time = cal.getTime
    val date = dateFmt.format(time)

    val dataframe = spark.read.option("sep", "|").schema(MyUtils.schema).csv(PATH.append(date).toString()).withColumn("AP", MyUtils.getAP(input_file_name))
//    println(s"dataframe getNumPartitions: ${dataframe.rdd.getNumPartitions}")
    val filterDs = dataframe.filter($"userMacAddr".equalTo("4C8BEF352A08"))
//    filterDs.show(2000, false)
//    println(s"filterDs getNumPartitions: ${filterDs.rdd.getNumPartitions}")
    val meanDs = filterDs.filter($"rssi".gt(-90)).groupBy("userMacAddr", "ts", "AP").agg(round(mean("rssi")).alias("rssi")).orderBy("userMacAddr", "AP", "ts")
//    println(s"meanDs getNumPartitions: ${meanDs.rdd.getNumPartitions}")
    val zipDf = meanDs.coalesce(4).rdd.zipWithIndex().map(tup=>MyUtils.dataWithId(tup._2, tup._1.getAs[String]("userMacAddr"), tup._1.getAs[Double]("rssi"), tup._1.getAs[Long]("ts"), tup._1.getAs[String]("AP"))).toDF()
    val groupDf = MyUtils.addColGroupid(zipDf)
    MyUtils.convertTimestampToDatetime(groupDf).orderBy("ts", "AP").show(2000, false)
    groupDf.createOrReplaceTempView("data0")
    groupDf.createOrReplaceTempView("data1")
    groupDf.createOrReplaceTempView("data2")
    val trajSql = "SELECT data1.userMacAddr, data1.ts, data1.AP, data1.groupid, data1.rssi FROM data1, data0, data2 WHERE data1.userMacAddr = data0.userMacAddr AND data1.AP = data0.AP AND data1._id - data0._id = 1 AND data1.rssi > data0.rssi AND data1.userMacAddr = data2.userMacAddr AND data1.AP = data2.AP AND data2._id - data1._id = 1 AND data1.rssi > data2.rssi"
    val trajDs = spark.sql(trajSql).coalesce(4)
    println(s"trajDs getNumPartitions: ${trajDs.rdd.getNumPartitions}")
    val resDf = MyUtils.convertTimestampToDatetime(trajDs).orderBy("ts", "groupid")
    resDf.show(2000, false)
  }
}
