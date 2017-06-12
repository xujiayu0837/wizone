import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{lit, mean, kurtosis}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, StringBuilder}

/**
  * Created by xujiayu on 17/5/17.
  */
object filterUnknownMACAddr {
  val FILENAME = "/Users/xujiayu/python/20170220"
  val FILENAME0 = "hdfs://10.103.93.27:9000/scandata/14E4E6E186A4/201705*"
  val FILENAME1 = "hdfs://10.103.93.27:9000/scandata/EC172FE3B340/201705*"
  val OUIFILENAME = new StringBuilder("/Users/xujiayu/Downloads/oui_new.txt")
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
    val spark = SparkSession.builder().appName("filterUnknownMACAddr").master("local[*]").config(new SparkConf()).getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import spark.implicits._
    val multiPaths = getPaths(DATAPATH, "5")
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
        new StringBuilder(content.toString).append("|").append(apMacAddr).toString()
        //        new StringBuilder(content.toString).append("|").append(groupid).toString()
      })
    }).persist()
//    println(s"before cnt: ${hadoopRdd.count()}")

    val ouiRdd = spark.sparkContext.textFile(OUIFILENAME.toString()).map(_.substring(0, 6)).persist()
    val ouiArr = ouiRdd.collect()
    println(s"ouiArr: ${ouiArr.toBuffer}")
    val broadcastRdd = spark.sparkContext.broadcast(ouiArr)

    val schema = StructType(
      List(
        StructField("userMacAddr", StringType, true),
        StructField("rssi", DoubleType, true),
        StructField("ts", StringType, true)
      )
    )
    val csvDf = spark.read.option("sep", "|").schema(schema).csv(FILENAME0, FILENAME1)
    val apStr = FILENAME0.split("/")(4)
    println(s"apStr: $apStr")
    val dataDf = csvDf.withColumn("groupid", lit(apStr))
    val groupDf = dataDf.groupBy($"userMacAddr", $"ts", $"groupid").agg(mean($"rssi")).orderBy($"userMacAddr")
    val resDf = groupDf.groupBy($"userMacAddr", $"groupid").agg(kurtosis($"avg(rssi)")).orderBy($"userMacAddr")
    groupDf.show(2000, false)
//    resDf.show(2000, false)

    val filterRdd = dataDf.rdd.filter{case Row(userMacAddr: String, rssi: Double, ts: String, groupid: String)=>broadcastRdd.value.contains(userMacAddr.substring(0, 6))}
    val filterDf = spark.createDataFrame(filterRdd, schema.add(StructField("groupid", StringType, true)))
//    filterDf.show(2000, false)
//    println(s"before cnt: ${csvDf.count()}")
//    println(s"after cnt: ${filterDf.count()}")
    println(s"csvDf getNumPartitions: ${csvDf.rdd.getNumPartitions}")
    println(s"filterDf getNumPartitions: ${filterDf.rdd.getNumPartitions}")
    println(s"FILENAME1 getNumPartitions: ${spark.read.option("sep", "|").schema(schema).csv(FILENAME1).rdd.getNumPartitions}")

//    println(s"after cnt: ${hadoopRdd.filter(line=>broadcastRdd.value.contains(line.split("[|]")(0).substring(0, 6))).count()}")
//    val dataRdd = hadoopRdd.filter(_.split("[|]").length == 4).filter(line=>{
//      val arr = line.split("[|]")
//      val subAddr = arr(0).substring(0, 6)
//      broadcastRdd.value.contains(subAddr)
//    }).map(_.split("[|]"))
//    val tmpRdd = dataRdd.map(arr=>Row(arr(0).trim, arr(1).toDouble, new Timestamp(arr(2).toLong * 1000L), arr(3).trim))
//    val schema = StructType(
//      List(
//        StructField("userMacAddr", StringType, true),
//        StructField("rssi", DoubleType, true),
//        StructField("ts", TimestampType, true),
//        StructField("groupid", StringType, true)
//      )
//    )
//    val dataDf = spark.createDataFrame(tmpRdd, schema)
//    val dataDs = dataDf.as[data]

    spark.stop()
  }
}

case class data(userMacAddr: String, rssi: Double, ts: Timestamp, groupid: String)