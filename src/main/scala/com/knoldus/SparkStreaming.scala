package com.knoldus

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This is a sample application which works on custom receiver.
  * Here we are using spark stream for reading data from file system.
  * It reads lines from file and prints lines which contains word 'programing'
  */
object SparkStreaming extends App{

  val conf: SparkConf = new SparkConf().setAppName("Spark-Streaming").setMaster("local[4]")
  val sc: SparkContext = new SparkContext(conf)
  val streamingContext = new StreamingContext(sc, Seconds(5))
  val host = "localhost"
  val port = 10000

  val customReceiverStream = streamingContext.receiverStream(new CustomReceiver(host, port))
  val words: DStream[String] = customReceiverStream.flatMap(_.split("\n"))

  val queryWord = "programming"

  val res: DStream[String] = words.filter(line => line.contains(queryWord))
  val count = res.count()

  println(s"\n\nLines with word: ${queryWord} are as follows: ")
  res.print()

  println("\nTotal occurrences found:  ")
  count.print()

  streamingContext.start()
  streamingContext.awaitTermination()
}