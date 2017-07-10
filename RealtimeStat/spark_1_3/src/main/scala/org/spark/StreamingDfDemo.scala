package org.spark

import java.sql.{Connection, Timestamp}
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, count, explode, from_unixtime, lag, lead, lit, max, mean, round, when}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable

/**
  * Created by xujiayu on 17/6/6.
  */
class MysqlPool extends Serializable {
  private val pool: ComboPooledDataSource = new ComboPooledDataSource(true)
  try {
    pool.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/wibupt?verifyServerCertificate=false&useSSL=true")
    pool.setDriverClass("com.mysql.jdbc.Driver")
    pool.setUser("root")
    pool.setPassword("root")
    pool.setMaxPoolSize(200)
  } catch {
    case e: Exception => e.printStackTrace()
  }
  def getConnection: Connection = {
    try {
      return pool.getConnection
    } catch {
      case e: Exception => e.printStackTrace()
        return null
    }
  }
}
object MysqlManager {
  var mysqlManager: MysqlPool = _
  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    return mysqlManager
  }
}
object StreamingDfDemo {
  val THRESHOLD = 1800
  val durations = 1
  val numCores = 4
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")
  val OUIFILENAME = new StringBuilder("/Users/xujiayu/Downloads/oui_new.txt")
  //  val PARQUETPATH = new StringBuilder("/home/hadoop/parquet")
  //  val OUIFILENAME = new StringBuilder("/home/hadoop/oui_new.txt")

  def stateSpecWordCount(key: String, value: Option[Int], state: State[Int]) = {
    val res = state.getOption().getOrElse(0) + value.getOrElse(0)
    val getState = state.getOption().getOrElse(0)
    val getValue = value.getOrElse(0)
    println(s"getState: $getState")
    println(s"getValue: $getValue")
    state.update(res)
    (key, res)
  }
  def stateSpec(key: String, value: Option[MyUtils.dataWithId], state: State[Seq[MyUtils.dataWithId]]) = {
    val getState = state.getOption().getOrElse(Seq[MyUtils.dataWithId]())
    val getValue = value.get
    val res = getState :+ getValue
    //    println(s"getState: $getState")
    //    println(s"getValue: $getValue")
    state.update(res)
    Some(res)
  }
  //  def stateSpec(key: String, value: Option[MyUtils.dataWithTs], state: State[Tuple2[Int, Seq[MyUtils.dataWithTs]]]) = {
  //    val getState = state.getOption().getOrElse(Tuple2[Int, Seq[MyUtils.dataWithTs]](0, Seq[MyUtils.dataWithTs]()))
  //    val getValue = value.get
  //    val sum = getState._1 + 1
  //    val seq = getState._2 :+ getValue
  //    println(s"getState1: ${getState._1}")
  //    println(s"getState2: ${getState._2}")
  //    println(s"getValue: ${getValue}")
  //    state.update(Tuple2[Int, Seq[MyUtils.dataWithTs]](sum, seq))
  //    Some(seq)
  //  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingDfDemo").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Minutes(durations))
    //    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getRootLogger.setLevel(Level.ERROR)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    ssc.checkpoint("/tmp/checkpoint")
    val Array(brokers, topics, jdbcMysql, mysqlUser, mysqlPasswd) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"->brokers
      //      "auto.offset.reset" -> "smallest"
    )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2).map(_.split("[|]")).filter(_.length == 5)
    //    val dataStream = lines.map(item=>data(item(0).trim, item(1).toDouble, new Timestamp(item(2).toLong * 1000L), item(3).trim))
    val dataStream = lines.map(item=>MyUtils.dataWithId(item(0).toLong, item(1).trim, item(2).toDouble, item(3).toLong, item(4).trim))
    val prop = new Properties()
    prop.put("user", mysqlUser)
    prop.put("password", mysqlPasswd)
//    val ouiDs = spark.read.option("sep", "|").csv(OUIFILENAME.toString()).map(_.getString(0))
//    val broadcastDs = broadcast(ouiDs)
    val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val blacklistArr = blacklistDs.collect()
//    val pairDstream = dataStream.map(item=>item.AP->item).mapWithState(StateSpec.function(stateSpec _).timeout(Seconds(1800)))
    //    val pairDstream = dataStream.transform(rdd=>rdd.zipWithIndex().map(tup=>MyUtils.dataWithDt(tup._2, tup._1.userMacAddr, tup._1.rssi, new Timestamp(tup._1.ts * 1000L), tup._1.AP))).map(item=>item->item).mapWithState(StateSpec.function(stateSpec _))
    //    pairDstream.print()
    dataStream.foreachRDD{rdd=>
      //      println(s"getNumPartitions: ${rdd.getNumPartitions}")
//      val dataDs = rdd.toDF().select(explode($"value").as("coll")).select($"coll.*").dropDuplicates()

//      val filterRdd = rdd.filter(arr=>arr(2).toInt.>(-90)).filter(arr => !blacklistArr.contains(arr(1))).map(arr=>((arr(1), arr(3), arr(4)), arr(2).toDouble)).mapValues(rss=>(rss, 1)).reduceByKey((tup0, tup1)=>(tup0._1+tup1._1, tup0._2+tup1._2)).mapValues(tup=>tup._1/tup._2).map(tup=>Array(tup._1._1, tup._2.round, tup._1._2, tup._1._3))
//      val groupRdd = MyUtils.rddAddColGroupid(filterRdd)
//      val modifyRdd = MyUtils.rddModifyColAP(groupRdd)
//      val peakWindowRdd = modifyRdd.groupBy(arr=>(arr(0), arr(3))).map(tup=>tup._2.toList.sortBy(arr=>(arr(0).toString, arr(3).toString, arr(2).toString)).zipWithIndex)
//      val prevNextRdd = MyUtils.rddAddColsPrevNext(peakWindowRdd)
//      val peakRdd = MyUtils.rddFilterColsPrevNext(prevNextRdd)
////      peakRdd.collect().foreach{arr=>
////        println(s"peakRdd: ${arr.toBuffer}")
////      }
//      val APWindowRdd = peakRdd.groupBy(arr=>(arr(0), arr(1), arr(2))).map(tup=>tup._2.toList.sortBy(arr=>(arr(0).toString, arr(1).toString, arr(2).toString, arr(3).toString)).zipWithIndex)
//      val APRdd = MyUtils.rddFilterColAPPrev(MyUtils.rddAddColAPPrev(APWindowRdd))
//      val rssWindowRdd = APRdd.groupBy(arr=>(arr(0), arr(2))).map(tup=>tup._2.toList.sortBy(arr=>(arr(0).toString, arr(2).toString, arr(1).toString)).zipWithIndex)
//      val trajRdd =  MyUtils.rddFilterColRssNext(MyUtils.rddAddColRssNext(rssWindowRdd))
//      trajRdd.collect().foreach{arr=>
//        println(s"trajRdd: ${arr.toBuffer}")
//      }
//      val windowRdd = trajRdd.groupBy(arr=>(arr(4), arr(0))).map(tup=>tup._2.toList.sortBy(arr=>(arr(4).toString, arr(0).toString, arr(2).toString)).zipWithIndex)
//      val comeGoRdd = MyUtils.rddAddColsPrev(windowRdd)
//      val comeRdd = MyUtils.rddFilterCome(comeGoRdd)
//      val goRdd = MyUtils.rddFilterGo(comeGoRdd)
//      val comeCount = comeRdd.groupBy(arr=>arr(4)).map(tup=>(tup._1, tup._2.toList.length))
//      val goCount = goRdd.groupBy(arr=>arr(4)).map(tup=>(tup._1, tup._2.toList.length))
//      comeCount.collect().foreach{tup=>
//        println(s"comeCount: ${tup._1}, ${tup._2}")
//      }
//      comeRdd.collect().foreach{arr=>
//        println(s"come: ${arr.toBuffer}")
//      }
//      goCount.collect().foreach{tup=>
//        println(s"goCount: ${tup._1}, ${tup._2}")
//      }
//      goRdd.collect().foreach{arr=>
//        println(s"go: ${arr.toBuffer}")
//      }
//      val joinRdd = comeCount.fullOuterJoin(goCount)
//      println(s"joinRdd: ${joinRdd.collect().toBuffer}")
//      val diffRdd = MyUtils.rddGetDiff(joinRdd)
//      println(s"diffRdd: ${diffRdd.collect().toBuffer}")
//
//      if (!diffRdd.isEmpty()) {
//        diffRdd.foreachPartition{par=>
//          val conn = MysqlManager.getMysqlManager.getConnection
////          conn.prepareStatement()
//          val stmt = conn.createStatement()
//          try {
//            conn.setAutoCommit(false)
//            par.foreach{data=>
//              val sql = "INSERT INTO realtime_statistic "
//              stmt.addBatch(sql)
//            }
//            conn.commit()
//          } catch {
//            case e: Exception => e.printStackTrace()
//          } finally {
//            stmt.close()
//            conn.close()
//          }
//        }
//      }

//      prevNextRdd.collect().foreach{list=>
//        list.foreach {arr=>
//          println(s"${arr.toBuffer}")
//        }
//      }
//      APWindowRdd.collect().foreach{list=>
//        list.foreach {tup=>
//            println(s"${tup._2}: ${tup._1.toBuffer}")
//        }
//      }

      val dataDs = rdd.toDF().repartition(numCores).persist(StorageLevel.MEMORY_AND_DISK_SER)
//      val filterDs = dataDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).filter($"userMacAddr".substr(0, 6).isin(broadcastDs.collect():_*)).groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi")).orderBy($"userMacAddr", $"AP", $"ts")
      val filterDs = dataDs.filter($"rssi".gt(-90)).filter(!$"userMacAddr".isin(blacklistDs.collect():_*)).coalesce(numCores)
      val meanDs = filterDs.groupBy($"userMacAddr", $"ts", $"AP").agg(round(mean($"rssi")).alias("rssi"))
//      val zipDf = filterDs.rdd.zipWithIndex().map(tup=>MyUtils.dataWithId(tup._2, tup._1.getAs[String]("userMacAddr"), tup._1.getAs[Double]("rssi"), tup._1.getAs[Long]("ts"), tup._1.getAs[String]("AP"))).toDF()
//      val groupDf = MyUtils.addColGroupid(zipDf)
//      val modifyDf = MyUtils.modifyColAP(groupDf).persist(StorageLevel.MEMORY_AND_DISK_SER)
//      //      modifyDf.show(2000, false)
//      modifyDf.createOrReplaceTempView("data0")
//      modifyDf.createOrReplaceTempView("data1")
//      modifyDf.createOrReplaceTempView("data2")
//      // 获取轨迹序列
//      val peakSql = "SELECT data1.userMacAddr, data1.ts, data1.AP, data1.groupid, data1.rssi FROM data1, data0, data2 WHERE data1.userMacAddr = data0.userMacAddr AND data1.AP = data0.AP AND data1._id - data0._id = 1 AND data1.rssi > data0.rssi AND data1.userMacAddr = data2.userMacAddr AND data1.AP = data2.AP AND data2._id - data1._id = 1 AND data1.rssi > data2.rssi"
      // 获取轨迹序列模块
      val peakWindow = Window.partitionBy("userMacAddr", "AP").orderBy("userMacAddr", "AP", "ts")
      val prevNextDf = MyUtils.addColsPrevNext(meanDs, peakWindow)
      val groupDf = MyUtils.addColGroupid(prevNextDf)
      val modifyDf = MyUtils.modifyColAP(groupDf)
      modifyDf.createOrReplaceTempView("data0")
//      val countSql = "SELECT data0.userMacAddr, data0.ts, data0.rssi, data0.AP, data0.groupid, data40.count FROM data0, (SELECT userMacAddr, ts, rssi, COUNT(*) AS count FROM data0 GROUP BY userMacAddr, ts, rssi) data40 WHERE data0.userMacAddr = data40.userMacAddr AND data0.ts = data40.ts AND data0.rssi = data40.rssi AND data40.count > 1"
//      spark.sql(countSql).orderBy("userMacAddr", "ts", "rssi").show(2000, false)
      val peakSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data0 WHERE (rssi > rssiPrev AND rssi > rssiNext) OR (rssi > rssiPrev AND rssiNext IS NULL) OR (rssiPrev IS NULL AND rssi > rssiNext)"
      println("-"*50+"start"+"-"*50)
      val peakDs = spark.sql(peakSql)
      println(peakSql)
      println("-"*50+"end"+"-"*50)
      val APWindow = Window.partitionBy("userMacAddr", "ts", "rssi").orderBy("userMacAddr", "ts", "rssi", "AP")
      MyUtils.addColAPPrev(peakDs, APWindow).createOrReplaceTempView("data30")
      val APSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data30 WHERE apPrev IS NULL"
      println("-"*50+"start"+"-"*50)
      val APDs = spark.sql(APSql)
      println(APSql)
      println("-"*50+"end"+"-"*50)
      val rssWindow = Window.partitionBy("userMacAddr", "ts").orderBy("userMacAddr", "ts", "rssi")
      MyUtils.addColRssNext(APDs, rssWindow).createOrReplaceTempView("data31")
      val rssSql = "SELECT userMacAddr, ts, AP, groupid, rssi FROM data31 WHERE rssNext IS NULL"
      println("-"*50+"start"+"-"*50)
      val trajDs = spark.sql(rssSql)
      println(rssSql)
      println("-"*50+"end"+"-"*50)
      trajDs.orderBy("groupid", "userMacAddr", "ts").show(2000, false)

//      val zipTrajDf = peakDs.rdd.zipWithIndex().map(tup=>MyUtils.data(tup._2, tup._1.getAs[String]("userMacAddr"), tup._1.getAs[Double]("rssi"), tup._1.getAs[Long]("ts"), tup._1.getAs[String]("AP"), tup._1.getAs[Int]("groupid"))).toDF().persist(StorageLevel.MEMORY_AND_DISK_SER)
//      zipTrajDf.createOrReplaceTempView("data10")
//      zipTrajDf.createOrReplaceTempView("data11")
//      // 获取进出大楼人数
//      val comeSql = s"SELECT data10.userMacAddr, data10.ts, data10.AP, data10.rssi, data11.ts, data11.AP, data11.rssi, data10.groupid FROM data10, data11 WHERE data10.userMacAddr = data11.userMacAddr AND data10.groupid = data11.groupid AND data11._id - data10._id = 1 AND data11.ts - data10.ts <= $THRESHOLD AND data10.AP = 'outside' AND data11.AP = 'inside'"
//      val goSql = s"SELECT data10.userMacAddr, data10.ts, data10.AP, data10.rssi, data11.ts, data11.AP, data11.rssi, data10.groupid FROM data10, data11 WHERE data10.userMacAddr = data11.userMacAddr AND data10.groupid = data11.groupid AND data11._id - data10._id = 1 AND data11.ts - data10.ts <= $THRESHOLD AND data10.AP = 'inside' AND data11.AP = 'outside'"
      // 获取进出大楼人数
      val window = Window.partitionBy("groupid", "userMacAddr").orderBy("groupid", "userMacAddr", "ts")
      MyUtils.addColsPrev(trajDs, window).createOrReplaceTempView("data10")
      val comeSql = "SELECT userMacAddr, tsPrev, apPrev, rssiPrev, ts, AP, rssi, groupid FROM data10 WHERE AP = 'inside' AND apPrev = 'outside'"
      val goSql = "SELECT userMacAddr, tsPrev, apPrev, rssiPrev, ts, AP, rssi, groupid FROM data10 WHERE AP = 'outside' AND apPrev = 'inside'"
      println("-"*50+"start"+"-"*50)
      spark.sql(comeSql).createOrReplaceTempView("comeData")
      println(comeSql)
      println("-"*50+"end"+"-"*50)
      println("-"*50+"start"+"-"*50)
      spark.sql(goSql).createOrReplaceTempView("goData")
      println(goSql)
      println("-"*50+"end"+"-"*50)
      val sql1 = "SELECT groupid, COUNT(groupid) AS comeCount FROM comeData GROUP BY groupid"
      val sql2 = "SELECT groupid, COUNT(groupid) AS goCount FROM goData GROUP BY groupid"
      val sql3 = "SELECT * FROM comeData"
      val sql4 = "SELECT * FROM goData"
      spark.sql(sql3).show(2000, false)
//      spark.sql(sql1).show(false)
      spark.sql(sql4).show(2000, false)
//      spark.sql(sql2).show(false)
      println("-"*50+"start"+"-"*50)
      val comeCount = spark.sql(sql1)
      println(sql1)
      println("-"*50+"end"+"-"*50)
      println("-"*50+"start"+"-"*50)
      val goCount = spark.sql(sql2)
      println(sql2)
      println("-"*50+"end"+"-"*50)
      val comeGoDs = comeCount.join(goCount, Seq("groupid"), "fullouter")
      //      comeGoDs.show(false)
      //      println(s"comeGoDs getNumPartitions: ${comeGoDs.rdd.getNumPartitions}")
      val newJoinDf = MyUtils.modifyColCount(comeGoDs)
      newJoinDf.createOrReplaceTempView("count")
      println("-"*50+"start"+"-"*50)
      val count = spark.sql("SELECT groupid, comeCount - goCount AS count FROM count")
      println("SELECT groupid, comeCount - goCount AS count FROM count")
      println("-"*50+"end"+"-"*50)
      count.show(2000, false)
      //      val resDf = MyUtils.addColMonTime(count)
      //      resDf.show(false)
      //      resDf.write.mode("append").jdbc("jdbc:mysql://10.103.93.27:3306/test", "realtime_statistic", prop)
      val tb0 = "(SELECT realtime_statistic.groupid, realtime_statistic.statistic AS base FROM realtime_statistic, (SELECT groupid, MAX(monTime) max FROM realtime_statistic GROUP BY groupid) data20 WHERE realtime_statistic.monTime = data20.max AND realtime_statistic.groupid = data20.groupid ORDER BY realtime_statistic.groupid) data21"
      try {
        val base = spark.read.jdbc(jdbcMysql, tb0, prop)
        //      base.show(2000, false)
        val joinDs = base.join(count, Seq("groupid"), "fullouter")
        MyUtils.modifyColBase(joinDs).createOrReplaceTempView("res")
        println("-" * 50 + "start" + "-" * 50)
        val resDf = MyUtils.addColMonTime(spark.sql("SELECT groupid, base + count AS statistic FROM res")).persist(StorageLevel.MEMORY_AND_DISK_SER)
        println("SELECT groupid, base + count AS statistic FROM res")
        println("-" * 50 + "end" + "-" * 50)
        resDf.show(2000, false)
        println(s"count: ${resDf.count()}")
        resDf.write.mode("append").jdbc(new StringBuilder(jdbcMysql).append("?verifyServerCertificate=false&useSSL=true").toString(), "realtime_statistic", prop)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}