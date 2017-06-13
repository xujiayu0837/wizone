package org.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by xujiayu on 17/6/13.
  */
object WriteMysqlDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WriteMysqlDemo").master("local[*]").getOrCreate()
    import spark.implicits._
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val df0 = Seq((5, -1, 1495597823), (1, -1, 1495597823)).toDF("groupid", "statistic", "monTime")
    df0.show(false)
//    df0.write.mode("append").jdbc("jdbc:mysql://10.103.93.27:3306/test", "realtime_statistic", prop)
  }
}
