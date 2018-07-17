package com.sparkstreaming

/**
  * Created by adity on 7/17/2018.
  */

import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.spark.SparkConf
object GetDepartmentTraffic {

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("streaming").setMaster(args(0))
    val ssc = new StreamingContext(conf, Seconds(10))
    val messages = ssc.socketTextStream(args(1),args(2).toInt)
    val depatmessages = messages.filter( msg => {
      val endPoint = msg.split(" ")(6)
      endPoint.split(("/")(1) == "department")
    })
    val departments = depatmessages.map(rec => {
      val endPoint = rec.split(" ")(6)
      (endPoint.split("/")(2),1)
    } )

    val departmentTra = departments.reduceByKey((total,value) => total + value)
    departmentTra.saveAsTextFiles("/user/cloud/Streaming/cnt")
  }



}
