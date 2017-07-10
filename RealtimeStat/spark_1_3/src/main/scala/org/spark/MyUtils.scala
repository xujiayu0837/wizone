package org.spark

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by xujiayu on 17/6/12.
  */
object MyUtils {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val groupid1 = Array("14E4E6E16E7A", "14E4E6E1867A")
  val groupid2 = Array("14E4E6E17A34", "14E4E6E172EA")
  val groupid3 = Array("388345A236BE", "5C63BFD90AE2")
  val groupid4 = Array("14E4E6E17648", "14E4E6E176C8")
  val groupid5 = Array("14E4E6E186A4", "EC172FE3B340")
  val groupid6 = Array("14E4E6E17908", "14E4E6E179F2")
  val groupid7 = Array("14E4E6E18658", "14E4E6E17950")
  val groupid8 = Array("14E4E6E1790A", "14E4E6E173FE")
  val groupid9 = Array("085700412D4E", "085700411A86")
  val groupid10 = Array("0C8268F15CB2", "0C8268F17FB8")
  // 学三
  val groupid19 = Array("0C8268C7E138", "0C8268EE3868")
  val groupid11 = Array("0C8268C804F8", "14CF924A98F2")
  val groupid12 = Array("0C8268EE3878", "0C8268EE7164")
  val groupid13 = Array("0C8268C7D518", "0C8268F17F60")
  val groupid14 = Array("0C8268EE3F32", "0857004127E2")
  val groupid15 = Array("0C8268F933A2", "0C8268F1648E")
  val groupid16 = Array("0C8268F90E64", "0C8268C7D504", "14E6E4E1C510", "0C8268C7DD6C")
  val groupid17 = Array("0C8268EE38EE", "0C8268F93B0A")
  val groupid18 = Array("0C8268F15C64", "0C8268F9314E")
  val schema = StructType(
    List(
      StructField("userMacAddr", StringType, true),
      StructField("rssi", DoubleType, true),
      StructField("ts", LongType, true)
    )
  )
  val getAP = udf{path: String => path.split("/")(4)}

  case class selectRssiData(userMacAddr: String, rssi: Double, ts: Timestamp, groupid: String)
  case class dataWithTs(userMacAddr: String, rssi: Double, ts: Long, AP: String)
  case class dataWithId(_id: Long, userMacAddr: String, rssi: Double, ts: Long, AP: String)
  case class dataWithDt(_id: Long, userMacAddr: String, rssi: Double, ts: Timestamp, AP: String)
  case class data(_id: Long, userMacAddr: String, rssi: Double, ts: Long, AP: String, groupid: Int)
  case class Trajectories(ts: Timestamp, AP: String)

  def addColGroupid(dataDf: DataFrame): DataFrame = {
    val groupDf = dataDf.withColumn("groupid", when($"AP".isin(MyUtils.groupid1:_*), lit(1))
      .when($"AP".isin(MyUtils.groupid2:_*), lit(2))
      .when($"AP".isin(MyUtils.groupid3:_*), lit(3))
      .when($"AP".isin(MyUtils.groupid4:_*), lit(4))
      .when($"AP".isin(MyUtils.groupid5:_*), lit(5))
      .when($"AP".isin(MyUtils.groupid6:_*), lit(6))
      .when($"AP".isin(MyUtils.groupid7:_*), lit(7))
      .when($"AP".isin(MyUtils.groupid8:_*), lit(8))
      .when($"AP".isin(MyUtils.groupid9:_*), lit(9))
      .when($"AP".isin(MyUtils.groupid10:_*), lit(10))
      .when($"AP".isin(MyUtils.groupid19:_*), lit(19))
      .when($"AP".isin(MyUtils.groupid11:_*), lit(11))
      .when($"AP".isin(MyUtils.groupid12:_*), lit(12))
      .when($"AP".isin(MyUtils.groupid13:_*), lit(13))
      .when($"AP".isin(MyUtils.groupid14:_*), lit(14))
      .when($"AP".isin(MyUtils.groupid15:_*), lit(15))
      .when($"AP".isin(MyUtils.groupid16:_*), lit(16))
      .when($"AP".isin(MyUtils.groupid17:_*), lit(17))
      .when($"AP".isin(MyUtils.groupid18:_*), lit(18))
      .otherwise(lit(0)))
    return groupDf
  }
  def addColHour(dataframe: DataFrame): DataFrame = {
    val hourDf = dataframe.withColumn("hour", hour($"ts"))
    return hourDf
  }
  def addColMonTime(dataframe: DataFrame): DataFrame = {
    val resDf = dataframe.withColumn("monTime", lit(System.currentTimeMillis().toString.substring(0, 10).toInt))
    return resDf
  }
  def addColsPrevNext(dataFrame: DataFrame, window: WindowSpec): DataFrame = {
    val rssiLag = lag("rssi", 1).over(window)
    val rssiLead = lead("rssi", 1).over(window)
    val lagLeadDf = dataFrame.withColumn("rssiPrev", rssiLag).withColumn("rssiNext", rssiLead)
    return lagLeadDf
  }
  def addColsPrev(dataFrame: DataFrame, window: WindowSpec): DataFrame = {
    val tsLag = lag("ts", 1).over(window)
    val APLag = lag("AP", 1).over(window)
    val rssiLag = lag("rssi", 1).over(window)
    val lagDf = dataFrame.withColumn("tsPrev", tsLag).withColumn("apPrev", APLag).withColumn("rssiPrev", rssiLag)
    return lagDf
  }
  def addColAPPrev(dataFrame: DataFrame, window: WindowSpec): DataFrame = {
    val APLag = lag("AP", 1).over(window)
    val lagDf = dataFrame.withColumn("apPrev", APLag)
    return lagDf
  }
  def addColRssNext(dataFrame: DataFrame, window: WindowSpec): DataFrame = {
    val rssLead = lead("rssi", 1).over(window)
    val leadDf = dataFrame.withColumn("rssNext", rssLead)
    return leadDf
  }
//  def modifyColAP(dataDf: DataFrame): DataFrame = {
//    val modifyDf = dataDf.withColumn("AP", when($"AP".isin(Array(groupid1(0), groupid2(0), groupid3(0), groupid4(0), groupid5(0), groupid6(0), groupid7(0), groupid8(0), groupid9(0), groupid10(0), groupid11(0), groupid12(0), groupid13(0), groupid14(0), groupid15(0), groupid16(0), groupid17(0), groupid18(0), groupid19(0)):_*), lit("outside"))
//      .when($"AP".isin(Array(groupid1(1), groupid2(1), groupid3(1), groupid4(1), groupid5(1), groupid6(1), groupid7(1), groupid8(1), groupid9(1), groupid10(1), groupid11(1), groupid12(1), groupid13(1), groupid14(1), groupid15(1), groupid16(1), groupid17(1), groupid18(1), groupid19(1)):_*), lit("inside"))
//      .when($"AP".isin(groupid16(2)), lit("outside1"))
//      .when($"AP".isin(groupid16(3)), lit("inside1"))
//      .otherwise(lit("-1")))
//    return modifyDf
//  }
  def modifyColAP(dataDf: DataFrame): DataFrame = {
    val modifyDf = dataDf.withColumn("AP", when($"AP".isin(Array(groupid1(0), groupid2(0), groupid3(0), groupid4(0), groupid5(0), groupid6(0), groupid7(0), groupid8(0), groupid9(0), groupid10(0), groupid11(0), groupid12(0), groupid13(0), groupid14(0), groupid15(0), groupid16(0), groupid16(2), groupid17(0), groupid18(0), groupid19(0)):_*), lit("outside"))
      .when($"AP".isin(Array(groupid1(1), groupid2(1), groupid3(1), groupid4(1), groupid5(1), groupid6(1), groupid7(1), groupid8(1), groupid9(1), groupid10(1), groupid11(1), groupid12(1), groupid13(1), groupid14(1), groupid15(1), groupid16(1), groupid16(3), groupid17(1), groupid18(1), groupid19(1)):_*), lit("inside"))
      .otherwise(lit("null")))
    return modifyDf
  }
  def modifyColCount(dataDf: DataFrame): DataFrame = {
    val newJoinDf = dataDf.withColumn("comeCount", when($"comeCount".isNull, lit(0)).otherwise($"comeCount")).withColumn("goCount", when($"goCount".isNull, lit(0)).otherwise($"goCount"))
    return newJoinDf
  }
  def modifyColBase(dataDf: DataFrame): DataFrame = {
    val modifyDf = dataDf.withColumn("count", when($"count".isNull, lit(0)).otherwise($"count")).withColumn("base", when($"base".isNull, lit(0)).otherwise($"base"))
    return modifyDf
  }
  def convertTimestampToDatetime(dataframe: DataFrame): DataFrame = {
    val datetimeDf = dataframe.withColumn("ts", from_unixtime($"ts"))
    return datetimeDf
  }
  def rddAddColGroupid(rdd: RDD[Array[Any]]): RDD[Array[Any]] = {
    return rdd.map{arr=>
      val arrBuf = arr.toBuffer
      val groupid = arrBuf(3) match {
        case "14E4E6E16E7A" => 1
        case "14E4E6E17A34" => 2
        case "388345A236BE" => 3
        case "14E4E6E17648" => 4
        case "14E4E6E186A4" => 5
        case "14E4E6E17908" => 6
        case "14E4E6E18658" => 7
        case "14E4E6E1790A" => 8
        case "085700412D4E" => 9
        case "0C8268F15CB2" => 10
        case "0C8268C7E138" => 19
        case "0C8268C804F8" => 11
        case "0C8268EE3878" => 12
        case "0C8268C7D518" => 13
        case "0C8268EE3F32" => 14
        case "0C8268F933A2" => 15
        case "0C8268F90E64" => 16
        case "14E6E4E1C510" => 16
        case "0C8268EE38EE" => 17
        case "0C8268F15C64" => 18
        case "14E4E6E1867A" => 1
        case "14E4E6E172EA" => 2
        case "5C63BFD90AE2" => 3
        case "14E4E6E176C8" => 4
        case "EC172FE3B340" => 5
        case "14E4E6E179F2" => 6
        case "14E4E6E17950" => 7
        case "14E4E6E173FE" => 8
        case "085700411A86" => 9
        case "0C8268F17FB8" => 10
        case "0C8268EE3868" => 19
        case "14CF924A98F2" => 11
        case "0C8268EE7164" => 12
        case "0C8268F17F60" => 13
        case "0857004127E2" => 14
        case "0C8268F1648E" => 15
        case "0C8268C7D504" => 16
        case "0C8268C7DD6C" => 16
        case "0C8268F93B0A" => 17
        case "0C8268F9314E" => 18
        case _ => 0
      }
      arrBuf.append(groupid)
      arrBuf.toArray
    }
  }
  def rddAddColsPrevNext(rdd: RDD[List[(Array[Any], Int)]]): RDD[Array[Any]] = {
    return rdd.map{list=>
      list.map{tup=>
        val end = list.length - 1
        val arrBuf = tup._1.toBuffer
        val _id = tup._2
        val rssLag = _id match {
          case 0 => "null"
          case _ => list(_id-1)._1(1)
        }
        val rssLead = _id match {
          case `end` => "null"
          case _ => list(_id+1)._1(1)

        }
        arrBuf.append(rssLag, rssLead)
        arrBuf.toArray
      }
    }.flatMap(item=>item)
  }
  def rddAddColsPrev(rdd: RDD[List[(Array[Any], Int)]]): RDD[Array[Any]] = {
    return rdd.map{list=>
      list.map{tup=>
        val arrBuf = tup._1.toBuffer
        val _id = tup._2
        val rssLead = _id match {
          case 0 => "null"
          case _ => list(_id-1)._1(1)
        }
        val tsLag = _id match {
          case 0 => "null"
          case _ => list(_id-1)._1(2)
        }
        val APLag = _id match {
          case 0 => "null"
          case _ => list(_id-1)._1(3)
        }
        arrBuf.append(rssLead, tsLag, APLag)
        arrBuf.toArray
      }
    }.flatMap(item=>item)
  }
  def rddAddColAPPrev(rdd: RDD[List[(Array[Any], Int)]]): RDD[Array[Any]] = {
    return rdd.map{list=>
      list.map{tup=>
        val arrBuf = tup._1.toBuffer
        val _id = tup._2
        val APLag = _id match {
          case 0 => "null"
          case _ => list(_id-1)._1(3)
        }
        arrBuf.append(APLag)
        arrBuf.toArray
      }
    }.flatMap(item=>item)
  }
  def rddAddColRssNext(rdd: RDD[List[(Array[Any], Int)]]): RDD[Array[Any]] = {
    return rdd.map{list=>
      list.map{tup=>
        val end = list.length - 1
        val arrBuf = tup._1.toBuffer
        val _id = tup._2
        val rssLead = _id match {
          case `end` => "null"
          case _ => list(_id+1)._1(1)
        }
        arrBuf.append(rssLead)
        arrBuf.toArray
      }
    }.flatMap(item=>item)
  }
  def rddFilterColsPrevNext(rdd: RDD[Array[Any]]): RDD[Array[Any]] = {
    return rdd.filter{arr=>
      val rss = arr(1).toString
      val rssLag = arr(5).toString
      val rssLead = arr(6).toString
      (rss.>(rssLag) && rss.>(rssLead)) || (rss.>(rssLag) && rssLead.equals("null")) || (rss.>(rssLead) && rssLag.equals("null"))
    }.map{arr=>
      val arrBuf = arr.toBuffer
      arrBuf.remove(5, 2)
      arrBuf.toArray
    }
  }
  def rddFilterColAPPrev(rdd: RDD[Array[Any]]): RDD[Array[Any]] = {
    return rdd.filter{arr=>
      val AP = arr(3).toString
      val APLag =  arr(5).toString
      APLag.equals("null")
    }.map{arr=>
      val arrBuf = arr.toBuffer
      arrBuf.remove(5)
      arrBuf.toArray
    }
  }
  def rddFilterColRssNext(rdd: RDD[Array[Any]]): RDD[Array[Any]] = {
    return rdd.filter{arr=>
      val rss = arr(1).toString
      val rssLead = arr(5).toString
      rssLead.equals("null")
    }.map{arr=>
      val arrBuf = arr.toBuffer
      arrBuf.remove(5)
      arrBuf.toArray
    }
  }
  def rddFilterCome(rdd: RDD[Array[Any]]): RDD[Array[Any]] = {
    return rdd.filter{arr=>
      val AP = arr(3).toString
      val APLag = arr(7).toString
      APLag.equals("outside") && AP.equals("inside")
    }
  }
  def rddFilterGo(rdd: RDD[Array[Any]]): RDD[Array[Any]] = {
    return rdd.filter{arr=>
      val AP = arr(3).toString
      val APLag = arr(7).toString
      APLag.equals("inside") && AP.equals("outside")
    }
  }
  def rddModifyColAP(rdd: RDD[Array[Any]]): RDD[Array[Any]] = {
    return rdd.map{arr=>
      val arrBuf = arr.toBuffer
      val AP = arrBuf(3) match {
        case "14E4E6E16E7A" => "outside"
        case "14E4E6E17A34" => "outside"
        case "388345A236BE" => "outside"
        case "14E4E6E17648" => "outside"
        case "14E4E6E186A4" => "outside"
        case "14E4E6E17908" => "outside"
        case "14E4E6E18658" => "outside"
        case "14E4E6E1790A" => "outside"
        case "085700412D4E" => "outside"
        case "0C8268F15CB2" => "outside"
        case "0C8268C7E138" => "outside"
        case "0C8268C804F8" => "outside"
        case "0C8268EE3878" => "outside"
        case "0C8268C7D518" => "outside"
        case "0C8268EE3F32" => "outside"
        case "0C8268F933A2" => "outside"
        case "0C8268F90E64" => "outside"
        case "14E6E4E1C510" => "outside"
        case "0C8268EE38EE" => "outside"
        case "0C8268F15C64" => "outside"
        case "14E4E6E1867A" => "inside"
        case "14E4E6E172EA" => "inside"
        case "5C63BFD90AE2" => "inside"
        case "14E4E6E176C8" => "inside"
        case "EC172FE3B340" => "inside"
        case "14E4E6E179F2" => "inside"
        case "14E4E6E17950" => "inside"
        case "14E4E6E173FE" => "inside"
        case "085700411A86" => "inside"
        case "0C8268F17FB8" => "inside"
        case "0C8268EE3868" => "inside"
        case "14CF924A98F2" => "inside"
        case "0C8268EE7164" => "inside"
        case "0C8268F17F60" => "inside"
        case "0857004127E2" => "inside"
        case "0C8268F1648E" => "inside"
        case "0C8268C7D504" => "inside"
        case "0C8268C7DD6C" => "inside"
        case "0C8268F93B0A" => "inside"
        case "0C8268F9314E" => "inside"
        case _ => "null"
      }
      arrBuf.remove(3)
      arrBuf.insert(3, AP)
      arrBuf.toArray
    }
  }
  def main(args: Array[String]): Unit = {

  }
}
