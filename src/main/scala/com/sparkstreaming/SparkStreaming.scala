package com.sparkstreaming

/**
  * Created by adity on 7/17/2018.
  */
import org.apache.spark.streaming._
import org.apache.spark.SparkConf

object SparkStreaming {
  def main (args: Array[String]) {

    val executionMode = args(0)
    val conf = new SparkConf().setAppName("streaming").setMaster("Yarn-client")
    val ssc = new StreamingContext(conf, Seconds(10))

    // Add lib dependencies Spark core and spark streaming to sbt file//
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream( args(1), args(2).toInt)
    // Split each line into words
    val words = lines.flatMap(line => line.split(" "))
    val tuples = words.map(word => (word, 1))
    val wordcount = tuples.reduceByKey((t, v) => t + v)
    wordcount.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }
}
