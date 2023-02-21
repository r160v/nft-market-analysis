package com.raphtory.nft.analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class NFT(fileOutput:String="/tmp/LOTRSixDegreesAnalyser") extends GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .select({ vertex =>
        if (vertex.getPropertyOrElse("address", otherwise = "nft") == "nft") {
          println("selecting rows")
          Row(
            vertex.getPropertyOrElse("nft", vertex.ID()),
            vertex.getPropertyOrElse("collection", "unknown"),
            vertex.getPropertyOrElse("category", "unknown"),
            vertex.getPropertyOrElse("priceUSD", "unknown")
          )


        } else {
          Row()
        }
      })
      .filter(row => row.getValues().length > 1)
      .writeTo(fileOutput)
  }
}

object NFT {
  def apply(path: String="/Users/r160/TFM/tfm/nft-analysis/src/main/scala/com/raphtory/nft/output/raphtory/NFT"):
  NFT = {
    new NFT(path + "NFT")
  }
}






