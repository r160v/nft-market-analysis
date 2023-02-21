package com.raphtory.nft.spouts

import com.raphtory.core.components.spout.Spout

import scala.collection.mutable

class NFTSpout(directory:String, filename: String) extends Spout[String] {
  //val filename = "/Users/r160/TFM/Examples/raphtory-example-lotr/src/main/scala/com/raphtory/examples/lotr/data/head5.csv"
  val fileQueue = mutable.Queue[String]()

  //extract the data and put it into a queue.
  override def setupDataSource(): Unit = {
    //val dtformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:m:ss")
    //val preproc = scala.io.Source.fromFile(directory + "/" + filename)
    //      .getLines.foreach(_.split(",").map(x => dtformat.parse(x).getTime))
    fileQueue++=
      scala.io.Source.fromFile(directory + "/" + filename)
        .getLines.drop(1)


  }

  // Sends each line of the file from the queue to the graph builder.
  // The generateData function runs whenever the graph builder requests data.
  // The crucial information that a line must contain is everything needed to build a node or edge or both.
  override def generateData(): Option[String] = {
    if(fileQueue isEmpty){
      dataSourceComplete() // to let the graph builders know not to pull any more data and so that Raphtory may fully
                           // synchronise, making the final timestamps available for analysis.
      None
    }
    else
      Some(fileQueue.dequeue())
  }
  override def closeDataSource(): Unit = {}//sometimes it's necessary to implement (e.g. Kafka streams or DB connections)
}
