package com.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume._

/**
  * Created by adity on 7/17/2018.
  */
object FlumeSparkStreaming {

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("streaming").setMaster(args(0))
    val ssc = new StreamingContext(conf, Seconds(10))
    val stream = FlumeUtils.createPollingStream(ssc,args(1),args(2).toInt)
    val messages = Stream.map(s => new String(s.event.getBody.array()))
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
