package com.raphtory.nft.analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class EdgeWeight(fileOutput:String="/tmp/LOTRSixDegreesAnalyser") extends GraphAlgorithm {



  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          println(s"${vertex.getPropertyOrElse("address",
            vertex.getPropertyOrElse("nft", "unknown"))}, **********")
          if (vertex.getPropertyOrElse("address", otherwise = "nft") == "nft") {
            val seller = vertex.getPropertyOrElse("seller", "unknown_seller")

            println(s"${vertex.getPropertyOrElse("address",
              vertex.getPropertyOrElse("nft", "unknown"))}, sellerID: $seller")
            vertex.messageAllIngoingNeighbors(seller)
          }
      })
      .select({ vertex =>
        if (vertex.getPropertyOrElse("address", otherwise = "nft") != "nft") {
          val queueList: List[(String, Int)] = vertex.messageQueue[String]
            .groupBy(el => el).map(e => (e._1, e._2.length)).toList

          println(s" in select ${vertex.getPropertyOrElse("address",
            vertex.getPropertyOrElse("nft", "unknown"))}, queueMap: $queueList")

          Row(vertex.getPropertyOrElse("address", vertex.ID()), queueList)

        } else Row()
        })
      .explode(row =>
        if (row.getValues().length > 1) {
          row.get(1).asInstanceOf[List[(String, Int)]].map(
            expl => Row(row(0), expl._1, expl._2)
          )
        }
        else List(Row())
      )
      .filter(row => row.getValues().length > 1)
      .writeTo(fileOutput)
  }
}

object EdgeWeight {
  def apply(path: String="/Users/r160/TFM/tfm/nft-analysis/src/main/scala/com/raphtory/nft/output/raphtory/EdgeWeight/"):
  EdgeWeight = {
    new EdgeWeight(path + "Inc1Win2")
  }
}






