package com.knoldus

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * This is a custom reciever which reads lines from a file
  * @param host
  * @param port
  */
class CustomReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){

  def onStart(){
    new Thread("Socket Receiver"){
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    print("Application stoped..!!")
  }

  private def receive() {
    try {
      val filePath = "/home/knoldus/fileExample"
      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))
      var userInput: String = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }

      reader.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}