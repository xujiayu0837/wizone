package org.spark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

/**
  * Created by xujiayu on 17/5/8.
  */
object SelectRssi {
  val DATAPATH = new StringBuilder("hdfs://10.103.93.27:9000/scandata/")
  val locMacMap = mutable.Map("1" -> ("14E4E6E16E7A", "14E4E6E1867A"), "2" -> ("14E4E6E17A34", "14E4E6E172EA"), "3" -> ("388345A236BE", "5C63BFD90AE2"), "4" -> ("14E4E6E17648", "14E4E6E176C8"), "5" -> ("14E4E6E186A4", "EC172FE3B340"), "6" -> ("14E4E6E17908", "14E4E6E179F2"), "7" -> ("14E4E6E17950", "14E4E6E18658"), "8" -> ("14E4E6E1790A", "14E4E6E173FE"), "9" -> ("085700412D4E", "085700411A86"), "10" -> ("0C8268F15CB2", "0C8268F17FB8"), "19" -> ("0C8268C7E138", "0C8268EE3868"), "11" -> ("0C8268C804F8", "14CF924A98F2"), "12" -> ("0C8268EE3878", "0C8268EE7164"), "13" -> ("0C8268C7D518", "0C8268F17F60"), "14" -> ("0C8268EE3F32", "0857004127E2"), "15" -> ("0C8268F933A2", "0C8268F1648E"), "16" -> ("0C8268F90E64", "0C8268C7D504", "14E6E4E1C510", "0C8268C7DD6C"), "17" -> ("0C8268EE38EE", "0C8268F93B0A"), "18" -> ("0C8268F15C64", "0C8268F9314E"))

  /**
    *
    * @param dataPath HDFS数据的路径
    * @param locStr 监测场所
    * @return 路径字符串
    */
  def getPaths(dataPath: StringBuilder, locStr: String = ""): String = {
    // 路径字符串
    val multiPaths = new StringBuilder
    // 存储相应日期的文件路径
    val arr_0 = new ArrayBuffer[String]()
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    // 获取locStr对应AP的数据
    var arr_1 = locMacMap.getOrElse(locStr, ()).toString.split("[^-\\w]+")
    // locStr合法,能获取到对应AP
    if (!arr_1.isEmpty) {
      arr_1 = arr_1.tail
      for (item <- arr_1) {
        val tmpPath = new StringBuilder
        tmpPath.append(dataPath).append(item).append("/").append(sdf.format(cal.getTime))
        arr_0 += tmpPath.toString
      }
      multiPaths.append(arr_0.mkString(","))
      return multiPaths.toString()
    }
    // 非法locStr返回空字符串
    else return ""
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf()).appName("SelectRssi").master("local[*]").getOrCreate()
    import spark.implicits._
//    val multiPaths = getPaths(DATAPATH, "5")
    val multiPaths = "hdfs://10.103.93.27:9000/scandata/14E4E6E186A4/2016*"
    val fc = classOf[TextInputFormat]
    val kc = classOf[LongWritable]
    val vc = classOf[Text]
    val hadoopConf = new Configuration()
    hadoopConf.set("defaultFS", "hdfs://10.103.93.27:9000")
    val originRdd = spark.sparkContext.newAPIHadoopFile(multiPaths, fc, kc, vc, hadoopConf)
    val hadoopRdd = originRdd.asInstanceOf[NewHadoopRDD[LongWritable, Text]].mapPartitionsWithInputSplit((inputSplit, it) => {
      val file = inputSplit.asInstanceOf[FileSplit]
      it.map(tup => {
        val content = tup._2
        val apMacAddr = file.getPath.toString.split('/')(4)
        val groupid = locMacMap.find(_._2.toString.contains(apMacAddr)).getOrElse(("", ()))._1
        new StringBuilder(content.toString).append("|").append(groupid).toString()
        //        new StringBuilder(content.toString).append("|").append(groupid).toString()
      })
    })
    val allRssiRdd = hadoopRdd.filter(_.split("[|]").length == 4).persist()
    println(s"getNumPartitions: ${allRssiRdd.getNumPartitions}")
//    println(s"cnt: ${allRssiRdd.count()}")
//    println(s"<= -90 cnt: ${allRssiRdd.filter(_.split("[|]")(1).toDouble <= -90).count()}")
//    println(s"<= -91 cnt: ${allRssiRdd.filter(_.split("[|]")(1).toDouble <= -91).count()}")
//    println(s"<= -89 cnt: ${allRssiRdd.filter(_.split("[|]")(1).toDouble <= -89).count()}")
    val resRdd = allRssiRdd.map(line=>{
      val arr = line.split("[|]")
      (arr(1).toDouble, 1)
    }).reduceByKey(_+_).sortBy(_._1)
//    println("resRdd: " + resRdd.collect().toBuffer)
    val dataRdd = hadoopRdd.filter(_.split("[|]").length == 4).filter(line => {
      val arr = line.split("[|]")
      arr(1).toDouble > -90
    }).map(_.split("[|]"))
    val tmpRdd = dataRdd.map(arr => Row(arr(0).trim, arr(1).toDouble, new Timestamp(arr(2).toLong * 1000L), arr(3).trim))
    // 传入数据类型参数
    val schema = StructType(
      List(
        StructField("userMacAddr", StringType, true),
        StructField("rssi", DoubleType, true),
        StructField("ts", TimestampType, true),
        StructField("groupid", StringType, true)
      )
    )
    val dataDf = spark.createDataFrame(tmpRdd, schema)
    val dataDs = dataDf.as[MyUtils.selectRssiData]
    val userId = dataDs.select("userMacAddr")
    userId.persist()

//    userId.show(2000, false)
//    println("cnt: "+userId.count())
//    println("distinct cnt: "+userId.rdd.distinct().count())
//    println("set size: "+userId.collect().toSet.size)
    val userIdSet = userId.collect().toSet
    for (item <- userIdSet) {
      val filter = dataDs.filter($"userMacAddr".equalTo(item.getString(0)))
    }
//    userId.rdd.foreach(item=>dataDs.filter($"userMacAddr".equalTo(item.getString(0))).show(2000, false))
//    userId.rdd.distinct().collect().foreach(item=>{
//      dataDs.filter($"userMacAddr".equalTo(item.toString())).select($"userMacAddr", $"rssi").toDF().foreach(println(_))
//    })

    userId.unpersist()

    spark.sparkContext.stop()
  }
}