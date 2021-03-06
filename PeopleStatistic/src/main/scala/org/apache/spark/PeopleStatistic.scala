package org.apache.spark

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Timer, TimerTask}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{first, lit}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xujiayu on 17/4/14.
  */
object PeopleStatistic {
  val locMacMap = mutable.Map("1" -> ("14E4E6E16E7A", "14E4E6E1867A"), "2" -> ("14E4E6E17A34", "14E4E6E172EA"), "3" -> ("388345A236BE", "5C63BFD90AE2"), "4" -> ("14E4E6E17648", "14E4E6E176C8"), "5" -> ("14E4E6E186A4", "EC172FE3B340"), "6" -> ("14E4E6E17908", "14E4E6E179F2"), "7" -> ("14E4E6E17950", "14E4E6E18658"), "8" -> ("14E4E6E1790A", "14E4E6E173FE"), "9" -> ("085700412D4E", "085700411A86"), "10" -> ("0C8268F15CB2", "0C8268F17FB8"), "19" -> ("0C8268C7E138", "0C8268EE3868"), "11" -> ("0C8268C804F8", ""), "12" -> ("0C8268EE3878", "0C8268EE7164"), "13" -> ("0C8268C7D518", "0C8268F17F60"), "14" -> ("0C8268EE3F32", "0857004127E2"), "15" -> ("0C8268F933A2", "0C8268F1648E"), "16" -> ("0C8268F90E64", "0C8268C7D504", "14E6E4E1C510", "0C8268C7DD6C"), "17" -> ("0C8268EE38EE", "0C8268F93B0A"), "18" -> ("0C8268F15C64", "0C8268F9314E"))
  // 数据的路径
  val DATAPATH = new StringBuilder("hdfs://10.103.93.27:9000/scandata/")
  // 测试
//  val DESTPATH = "/tmp/idea_print/spark_1_2/170424_test_"
  val DESTPATH = "tmp/realtime_statistic/groupid_"

  /**
    *
    * @param groupid 监测场所
    * @param spark spark环境
    * @return spark封装原始数据
    */
  def getDataDs(groupid: Int, spark: SparkSession): Dataset[data] = {
    import spark.implicits._
    // 获取路径
    // 测试
//    val multiPaths = MyUtils.readData(DATAPATH, 1, groupid.toString)
    val multiPaths = MyUtils.getPaths(DATAPATH, groupid.toString)
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
    val dataDs = dataDf.as[data]
    return dataDs
  }

  /**
    *
    * @param groupid 监测场所
    * @param spark spark环境
    * @return 探测表
    */
  def init(groupid: Int, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val dataDs = getDataDs(groupid, spark)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    // 初始化探测表
    val detectDs = dataDs.filter($"ts"<=new Timestamp(System.currentTimeMillis())).filter($"ts">new Timestamp(System.currentTimeMillis()-5*60*1000L)).groupBy("userMacAddr", "groupid").agg(first("ts").as("ts")).orderBy("ts")
//    detectDs.show(2000, truncate = false)
    // 初始化到访记录表
    val visitRecordDs = detectDs.withColumnRenamed("ts", "startTime").withColumn("endTime", lit(null))

    println("*"*50 + "initial run: " + "*"*50)
    // 到访记录表存为临时文件
    visitRecordDs.rdd.saveAsTextFile(DESTPATH+groupid+"")

    return detectDs
  }

  /**
    *
    * @param groupid 监测场所
    * @param spark spark环境
    * @return 刷新后的探测表
    */
  def refreshDetectDs(groupid: Int, spark: SparkSession): DataFrame = {
    import spark.implicits._
    println("*"*50 + "timer run: " + "*"*50)
    val dataDs = getDataDs(groupid, spark)
//    dataDs.persist()
    // 每5分钟刷新探测表
    return dataDs.filter($"ts"<=new Timestamp(System.currentTimeMillis())).filter($"ts">new Timestamp(System.currentTimeMillis()-5*60*1000L)).groupBy("userMacAddr", "groupid").agg(first("ts").as("ts")).orderBy("ts")

//    println("detectDs:")
//    detectDs.show(2000, truncate = false)
  }

  /**
    *
    * @param groupid 监测场所
    * @param detectDs 探测表
    * @param spark spark环境
    * @return 更新的到访记录表
    */
  def updateVisitRecord(groupid: Int, detectDs: DataFrame, spark: SparkSession): RDD[String] = {
    import spark.implicits._
    val visitRecordRdd = spark.sparkContext.textFile(DESTPATH+groupid+"").cache()
    val visitArr = MyUtils.rdd2arr(visitRecordRdd)

    val detectDf = detectDs.select("userMacAddr")
    val detectArr = detectDf.map(item=>{
      item(0).asInstanceOf[String]
    }).collect()

    val detectDiffVisit = detectArr.diff(visitArr)
    //      var arr = new ArrayBuffer[DataFrame]
    var strArr = new ArrayBuffer[String]

    //      println("buf: " + detectArrBuf.length + " " + visitArrBuf.length)
    //      println("detectDiffVisit: " + detectDiffVisit)

    for (item <- detectDiffVisit) {
      //        arr += detectDs.filter($"userMacAddr".equalTo(item)).select("ts").toDF().first().getTimestamp(0).toString
      val ts = detectDs.filter($"userMacAddr".equalTo(item)).select("ts").toDF().first().getTimestamp(0)
      //        println("ts: " + ts)
      val groupid = detectDs.filter($"userMacAddr".equalTo(item)).select("groupid").toDF().first().getString(0)
      //        println("groupid: " + groupid)
      //        strArr += "["+ts.toString()+","+groupid.toString()+","+"null"+"]"
      strArr += "["+item+","+ts.toString+","+groupid.toString+","+"null"+"]"
      //        println("strArr: " + strArr)
    }
    val newVisitRdd = visitRecordRdd ++ spark.sparkContext.makeRDD(strArr)
    val newVisitArr = MyUtils.rdd2arr(newVisitRdd)
    val visitDiffDetect = newVisitArr.diff(detectArr)
    //      println("visitDiffDetect: " + visitDiffDetect.toString)
    val updateRdd = newVisitRdd.map(line=>{
      val arr = MyUtils.stripChars(MyUtils.stripChars(line, "["), "]").split(",")
      //        println("arr0: " + arr(0))
      if (visitDiffDetect.contains(arr(0))) {
        arr(3) = System.currentTimeMillis().toString
        var newLine = "["+arr.mkString(",")+"]"
        //          println("newLine: " + arr(3))
        newLine
      }
      else {
        line
      }
      //        "["+arr(0)+","+arr(1)+","+arr(2)+","+arr(3)+","+"]"

    })
    if (updateRdd == newVisitRdd) {
      println("No update")
    }
    return updateRdd
  }

  /**
    *
    * @param groupid 监测场所
    * @param updateRdd 到访记录表
    * @param detectDs 探测表
    * @param args mysql用户名以及密码
    */
  def getResult(groupid: Int, updateRdd: RDD[String], detectDs: DataFrame, args: Array[String]): Unit = {
    Thread.sleep(5*1000L)
    val cnt = updateRdd.filter(line=>{
      val arr = MyUtils.stripChars(MyUtils.stripChars(line, "["), "]").split(",")
//      println("arr3: " + arr(3))
      arr(3) == "null"
    }).count()
    println("groupid: " + groupid + ", cnt: " + cnt)
    MyUtils.insertTable(args(0), args(1), groupid, cnt)
  }

  /**
    *
    * @param groupid 监测场所
    * @param updateRdd 到访记录表
    */
  def saveVisitRecord(groupid: Int, updateRdd: RDD[String]): Unit = {
    println("*"*50 + "dirdel run: " + "*"*50)
    MyUtils.dirDel(new File(DESTPATH+groupid+""))

    println("*"*50 + "dirsave run: " + "*"*50)
//    println("updateRdd: " + updateRdd.collect().toBuffer)

    updateRdd.saveAsTextFile(DESTPATH+groupid+"")
  }

  /**
    *
    * @param groupid 监测场所
    * @param detectDs 探测表
    * @param args mysql用户名以及密码
    * @param spark spark环境
    */
  class MyTimerTask(groupid: Int, var detectDs: DataFrame, args: Array[String], spark: SparkSession) extends TimerTask {
    val memberSess = spark
    override def run() = {
      detectDs = refreshDetectDs(groupid, memberSess)
      val updateRdd = updateVisitRecord(groupid, detectDs, memberSess)
      getResult(groupid, updateRdd, detectDs, args)
      saveVisitRecord(groupid, updateRdd)
    }
  }

  def main(args: Array[String]): Unit = {
    // 测试
    val spark = SparkSession.builder().config(new SparkConf()).appName("PeopleStatistic").master("local[*]").getOrCreate()
//    val spark = SparkSession.builder().config(new SparkConf()).appName("PeopleStatistic").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    try {
      for (i <- (1 until(20))) {
        val detectDs = init(i, spark)

        val task = new MyTimerTask(i, detectDs, args, spark)
        val t = new Timer()
        // 测试
        t.schedule(task, 0L, 30 * 1000L)
//        t.schedule(task, 0L, 5 * 60 * 1000L)
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
//    spark.sparkContext.stop()
  }
}

case class data(userMacAddr: String, rssi: Double, ts: Timestamp, groupid: String)