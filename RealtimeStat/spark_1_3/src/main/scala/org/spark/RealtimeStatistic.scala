package org.spark

import java.sql.Connection
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, explode, mean, round}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._

import scala.collection.mutable.StringBuilder

/**
  * Created by xujiayu on 17/6/15.
  */
//class MysqlPool extends Serializable {
//  private val pool: ComboPooledDataSource = new ComboPooledDataSource(true)
//  try {
//    pool.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/wibupt?verifyServerCertificate=false&useSSL=true")
//    pool.setDriverClass("com.mysql.jdbc.Driver")
//    pool.setUser("root")
//    pool.setPassword("root")
//    pool.setMaxPoolSize(200)
//  } catch {
//    case e: Exception => e.printStackTrace()
//  }
//  def getConnection: Connection = {
//    try {
//      return pool.getConnection
//    } catch {
//      case e: Exception => e.printStackTrace()
//        return null
//    }
//  }
//}
//object MysqlManager {
//  var mysqlManager: MysqlPool = _
//  def getMysqlManager: MysqlPool = {
//    synchronized {
//      if (mysqlManager == null) {
//        mysqlManager = new MysqlPool
//      }
//    }
//    return mysqlManager
//  }
//}
object RealtimeStatistic {
  val THRESHOLD = 9273
  val durations = 1
  val numCores = 4
  val PARQUETPATH = new StringBuilder("/Users/xujiayu/parquet")
  val OUIFILENAME = new StringBuilder("/Users/xujiayu/Downloads/oui_new.txt")
  //  val PARQUETPATH = new StringBuilder("/home/hadoop/parquet")
  //  val OUIFILENAME = new StringBuilder("/home/hadoop/oui_new.txt")

  def insertDB(rdd: RDD[(Any, Int)]): Unit = {
    if (!rdd.isEmpty()) {
      val resRdd = MyUtils.rddAddColMonTime(rdd)
      resRdd.foreachPartition{par=>
        val conn = MysqlManager.getMysqlManager.getConnection
        val sql = "INSERT INTO realtime_statistic SET groupid = ?, statistic = ?, monTime = ?"
        val pstmt = conn.prepareStatement(sql)
        try {
          conn.setAutoCommit(false)
          par.foreach{tup=>
            pstmt.setObject(1, tup._1)
            pstmt.setObject(2, tup._2)
            pstmt.setObject(3, tup._3)
            // 如果使用addBatch(), sql语句中就能带"?"参数, 而addBatch(sql), sql语句中不能带"?"参数
            pstmt.addBatch()
          }
          //          pstmt.executeBatch()
          conn.commit()
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (pstmt != null) {
            pstmt.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      }
    }
  }
  def rddGetMean(rdd: RDD[Array[String]], blacklistArr: Array[String]): RDD[Array[Any]] = {
    val filterRdd = rdd.filter(arr=>arr(2).toInt.>(-90)).filter(arr => !blacklistArr.contains(arr(1))).coalesce(numCores)
    val meanRdd = filterRdd.map(arr=>((arr(1), arr(3), arr(4)), arr(2).toDouble)).mapValues(rss=>(rss, 1)).reduceByKey((tup0, tup1)=>(tup0._1+tup1._1, tup0._2+tup1._2)).mapValues(tup=>tup._1/tup._2).map(tup=>Array(tup._1._1, tup._2.round, tup._1._2, tup._1._3))
    return meanRdd
  }
  // 获取轨迹序列模块
  def rddGetTraj(meanRdd: RDD[Array[Any]]): RDD[Array[Any]] = {
    val groupRdd = MyUtils.rddAddColGroupid(meanRdd)
    val modifyRdd = MyUtils.rddModifyColAP(groupRdd)
    val peakWindowRdd = modifyRdd.groupBy(arr=>(arr(0), arr(3))).map(tup=>tup._2.toList.sortBy(arr=>(arr(0).toString, arr(3).toString, arr(2).toString)).zipWithIndex)
    val prevNextRdd = MyUtils.rddAddColsPrevNext(peakWindowRdd)
    val peakRdd = MyUtils.rddFilterColsPrevNext(prevNextRdd)
    //    peakRdd.collect().foreach{arr=>
    //      println(s"peakRdd: ${arr.toBuffer}")
    //    }
    val APWindowRdd = peakRdd.groupBy(arr=>(arr(0), arr(1), arr(2))).map(tup=>tup._2.toList.sortBy(arr=>(arr(0).toString, arr(1).toString, arr(2).toString, arr(3).toString)).zipWithIndex)
    val APRdd = MyUtils.rddFilterColAPPrev(MyUtils.rddAddColAPPrev(APWindowRdd))
    val rssWindowRdd = APRdd.groupBy(arr=>(arr(0), arr(2))).map(tup=>tup._2.toList.sortBy(arr=>(arr(0).toString, arr(2).toString, arr(1).toString)).zipWithIndex)
    val trajRdd =  MyUtils.rddFilterColRssNext(MyUtils.rddAddColRssNext(rssWindowRdd))
    return trajRdd
  }
  // 计算净增加人数模块
  def rddGetComeGo(trajRdd: RDD[Array[Any]]): RDD[(Any, Int)] = {
    val windowRdd = trajRdd.groupBy(arr=>(arr(4), arr(0))).map(tup=>tup._2.toList.sortBy(arr=>(arr(4).toString, arr(0).toString, arr(2).toString)).zipWithIndex)
    val comeGoRdd = MyUtils.rddAddColsPrev(windowRdd)
    val comeRdd = MyUtils.rddFilterCome(comeGoRdd)
    val goRdd = MyUtils.rddFilterGo(comeGoRdd)
    val comeCount = comeRdd.groupBy(arr=>arr(4)).map(tup=>(tup._1, tup._2.toList.length))
    val goCount = goRdd.groupBy(arr=>arr(4)).map(tup=>(tup._1, tup._2.toList.length))
    comeCount.collect().foreach{tup=>
      println(s"comeCount: ${tup._1}, ${tup._2}")
    }
    comeRdd.collect().foreach{arr=>
      println(s"come: ${arr.toBuffer}")
    }
    goCount.collect().foreach{tup=>
      println(s"goCount: ${tup._1}, ${tup._2}")
    }
    goRdd.collect().foreach{arr=>
      println(s"go: ${arr.toBuffer}")
    }
    val diffRdd = MyUtils.rddGetDiff(comeCount.fullOuterJoin(goCount))
    return diffRdd
  }
  // 获取当前人数
  def rddSaveRes(diffRdd: RDD[(Any, Int)]): Unit = {
    val conn = MysqlManager.getMysqlManager.getConnection
    val sql = "SELECT realtime_statistic.groupid AS groupid, realtime_statistic.statistic AS base FROM realtime_statistic, (SELECT groupid, MAX(monTime) max FROM realtime_statistic GROUP BY groupid) data20 WHERE data20.max > ? AND realtime_statistic.monTime = data20.max AND realtime_statistic.groupid = data20.groupid"
    val pstmt = conn.prepareStatement(sql)
    val baseArr = Array((0, 0))
    val baseBuf = baseArr.toBuffer
    try {
      pstmt.setObject(1, 1400000000)
      val rs = pstmt.executeQuery()
      if (!rs.isBeforeFirst) {
        insertDB(diffRdd)
      }
      else {
        while (rs.next()) {
          val groupid = rs.getInt("groupid")
          val base = rs.getInt("base")
          baseBuf.append((groupid, base))
          println(s"groupid: $groupid, base: $base")
        }
        val baseRdd = diffRdd.sparkContext.parallelize(baseBuf).filter(_._1 != 0)
        val countRdd = MyUtils.rddGetSum(diffRdd.map(tup=>(tup._1.toString.toInt, tup._2)).fullOuterJoin(baseRdd))
        println(s"countRdd: ${countRdd.collect().toBuffer}")
        insertDB(countRdd)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (pstmt != null) {
        pstmt.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
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
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RealtimeStatistic").setMaster("local[*]")
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
    val lines = messages.map(_._2).map(_.split("[|]"))
    //    val dataStream = lines.map(item=>data(item(0).trim, item(1).toDouble, new Timestamp(item(2).toLong * 1000L), item(3).trim))
    val dataStream = lines.map(item=>MyUtils.dataWithId(item(0).toLong, item(1).trim, item(2).toDouble, item(3).toLong, item(4).trim))
    val prop = new Properties()
    prop.put("user", mysqlUser)
    prop.put("password", mysqlPasswd)
    val ouiDs = spark.read.option("sep", "|").csv(OUIFILENAME.toString()).map(_.getString(0))
    val broadcastDs = broadcast(ouiDs)
    val blacklistDs = spark.read.parquet(PARQUETPATH.toString()).map(_.getString(0)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val blacklistArr = blacklistDs.collect()
    val pairDstream = dataStream.map(item=>item.AP->item).mapWithState(StateSpec.function(stateSpec _).timeout(Seconds(9273)))
    //    val pairDstream = dataStream.transform(rdd=>rdd.zipWithIndex().map(tup=>MyUtils.dataWithDt(tup._2, tup._1.userMacAddr, tup._1.rssi, new Timestamp(tup._1.ts * 1000L), tup._1.AP))).map(item=>item->item).mapWithState(StateSpec.function(stateSpec _))
    //    pairDstream.print()
    lines.foreachRDD{rdd=>
      val meanRdd = rddGetMean(rdd, blacklistArr)
      val trajRdd = rddGetTraj(meanRdd)
      trajRdd.collect().foreach{arr=>
        println(s"trajRdd: ${arr.toBuffer}")
      }
      val diffRdd = rddGetComeGo(trajRdd)
      println(s"diffRdd: ${diffRdd.collect().toBuffer}")
      rddSaveRes(diffRdd)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
