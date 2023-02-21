package com.raphtory.nft.analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class TraderNFTDegree(fileOutput:String="/tmp/LOTRSixDegreesAnalyser",
                      vertexType: NodeType) extends GraphAlgorithm {

//  def flatten(l: List[Any]): List[Any] = l flatMap {
//    case ls: List[_] => flatten(ls)
//    case h => List(h)
//  }

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          vertexType match {
            case NodeType.Trader =>
              if (vertex.getPropertyOrElse("address", otherwise = "nft") != "nft")
                vertex.messageAllIngoingNeighbors(0)
                vertex.messageAllOutgoingNeighbors(1)
            case NodeType.NFT =>
              if (vertex.getPropertyOrElse("address", otherwise = "nft") == "nft")
                vertex.messageAllIngoingNeighbors(0)
            case NodeType.TraderNFT =>
              vertex.setState("inDegree", vertex.getInNeighbours().size)
              vertex.setState("outDegree", vertex.getOutNeighbours().size)
              vertex.setState("totalDegree", vertex.getAllNeighbours().size)
          }
      })
      .iterate({
        vertex =>
          vertexType match {
            case NodeType.Trader =>
              //val queueList: List[Int] = flatten(vertex.messageQueue[Int].productIterator.toList).asInstanceOf[List[Int]]
//              val inDegree = queueList.sum
              //println(s"${vertex.getPropertyOrElse("address",
                //vertex.getPropertyOrElse("nft", "unknown"))}, inDeg: ${inDegree}, outDeg: $outDegree")
//              val outDegree = queueList.size - inDegree
              val queueMap: Map[Int, Int] = vertex.messageQueue[Int]
                .groupBy(mess => mess).map(tup => (tup._1, tup._2.length))

              val inDegree = queueMap.getOrElse(1, 0)
              val outDegree = queueMap.getOrElse(0, 0)

              vertex.setState("inDegree", inDegree)
              vertex.setState("outDegree", outDegree)
              vertex.setState("totalDegree", inDegree + outDegree)
            case NodeType.NFT =>
              val degree = vertex.messageQueue[Int].size
              vertex.setState("degree", degree)
              println(s"nft ${vertex.getPropertyOrElse("address",
                vertex.getPropertyOrElse("nft", "unknown"))}, outDeg: $degree")
            case _ =>
          }
      }, iterations = 1, executeMessagedOnly = true)
      .select({ vertex =>
        if (vertex.getPropertyOrElse("address", otherwise = "nft") != "nft") {

          println(s" in select ${vertex.getPropertyOrElse("address",
            vertex.getPropertyOrElse("nft", "unknown"))}")

          vertexType match {
            case NodeType.NFT =>
              Row(vertex.getPropertyOrElse("address", vertex.ID()),
                vertex.getStateOrElse("degree", 0))
            case _ =>
              Row(vertex.getPropertyOrElse("address", vertex.ID()),
                  vertex.getStateOrElse("inDegree", 0),
                  vertex.getStateOrElse("outDegree", 0),
                  vertex.getStateOrElse("totalDegree", 0))
          }
        } else Row()
        })
      .filter(row => row.getValues().length > 1)
      .writeTo(fileOutput)
  }
}

object TraderNFTDegree{
  def apply(path: String="/Users/r160/TFM/tfm/nft-analysis/src/main/scala/com/raphtory/nft/output/raphtory/TraderNFTDegree/",
            vertexType: NodeType): TraderNFTDegree = {
    new TraderNFTDegree(path + "TraderNFTDegree/" + vertexType, vertexType)
  }
}