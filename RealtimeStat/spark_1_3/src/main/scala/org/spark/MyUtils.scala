package org.spark

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, when, input_file_name, from_unixtime, udf, hour}
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
  val groupid7 = Array("14E4E6E17950", "14E4E6E18658")
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
  def modifyColAP(dataDf: DataFrame): DataFrame = {
    val modifyDf = dataDf.withColumn("AP", when($"AP".isin(Array(groupid1(0), groupid2(0), groupid3(0), groupid4(0), groupid5(0), groupid6(0), groupid7(0), groupid8(0), groupid9(0), groupid10(0), groupid11(0), groupid12(0), groupid13(0), groupid14(0), groupid15(0), groupid16(0), groupid17(0), groupid18(0), groupid19(0)):_*), lit("0"))
      .when($"AP".isin(Array(groupid1(1), groupid2(1), groupid3(1), groupid4(1), groupid5(1), groupid6(1), groupid7(1), groupid8(1), groupid9(1), groupid10(1), groupid11(1), groupid12(1), groupid13(1), groupid14(1), groupid15(1), groupid16(1), groupid17(1), groupid18(1), groupid19(1)):_*), lit("1"))
      .when($"AP".isin(groupid16(2)), lit("2"))
      .when($"AP".isin(groupid16(3)), lit("3"))
      .otherwise(lit("-1")))
    return modifyDf
  }
  def modifyCol(dataDf: DataFrame): DataFrame = {
    val newJoinDf = dataDf.withColumn("comeCount", when($"comeCount".isNull, lit(0)).otherwise($"comeCount")).withColumn("goCount", when($"goCount".isNull, lit(0)).otherwise($"goCount"))
    return newJoinDf
  }
  def convertTimestampToDatetime(dataframe: DataFrame): DataFrame = {
    val datetimeDf = dataframe.withColumn("ts", from_unixtime($"ts"))
    return datetimeDf
  }
  def main(args: Array[String]): Unit = {

  }
}
