package com.sparkstreaming

/**
  * Created by adity on 7/17/2018.
  */
import org.apache.commons.codec.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import Kafka.Serializer.StringDecoder

object KafkaSparkStreaming {

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("streaming").setMaster(args(0))
    val ssc = new StreamingContext(conf, Seconds(10))
    val KafkaParams = Map[String,String]("metadata.broker.list" -> "")
    val topicSet = Set("fkdemodg")
    val stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,KafkaParams,topicSet)
    val messages = Stream.map(s => s._2)
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



}
