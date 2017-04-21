import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xujiayu on 17/4/17.
  */
object ForLoopTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RealtimeTraffic").setMaster("local[*]")
    val sc = new SparkContext(conf)
    for (i <- (1 until(20))) {
      println("i++: " + i)
    }
    sc.stop()
  }
}
