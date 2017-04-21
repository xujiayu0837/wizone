package org.apache.spark

import java.util.{Timer, TimerTask}

/**
  * Created by xujiayu on 17/4/20.
  */
object QA {
  def stripChars(s: String, ch: String): String = {
    val str = s.filterNot(ch.contains(_))
    str
  }
  def str2arr(str: String): Array[String] = {
    val arr = str.split("[^-\\w]+").tail
    arr
  }
  def main(args: Array[String]): Unit = {
    val timer = new Timer()
    val task_0 = new TimerTask {
      override def run() = {
        println("*"*20)
      }
    }
    val task_1 = new TimerTask {
      override def run() = {
        println("#"*20)
      }
    }
    timer.schedule(task_0, 0L, 5*1000L)
    timer.schedule(task_1, 0L, 5*1000L)
//    val str = "(14E4E6E16E7A, 14E4E6E1867A)"
//    val arr_0 = stripChars(stripChars(str, "("), ")").split(",")
//    for (item <- arr_0) {
//      println("arr_0: " + item)
//    }
//    val arr_1 = str2arr(str)
//    for (item <- arr_1) {
//      println("arr_1: " + item)
//    }
  }
}
