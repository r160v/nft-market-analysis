package com.raphtory.nft.analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

import java.util.concurrent.TimeUnit


class TraderStrength(fileOutput:String="/tmp/TraderStrength") extends GraphAlgorithm {

  case class TraderInfo(toNode: String, nTransaction: Int, priceUSD: BigDecimal)
  case class CollectionCategory(toNode: String, collection: String, category: String)

  val dtformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:m:ss")

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          if (vertex.getPropertyOrElse("address", otherwise = "nft") == "nft") {
//            println(s"propertyHistories ${
//              vertex.getPropertyOrElse("address",
//                vertex.getPropertyOrElse("nft", "unknown"))
//            }, PropertyHistorySeller: ${vertex.getPropertyHistory("sellerID")}, " +
//              s"PropertyHistoryBuyer: ${vertex.getPropertyHistory("buyerID")}")

            val buyerList: List[Long] = vertex.getPropertyHistory("buyerID").get
              .asInstanceOf[List[(Long, Long)]]
              .map {
                tuple => tuple._2
              }

            val sellerList: List[Long] = vertex.getPropertyHistory("sellerID").get
              .asInstanceOf[List[(Long, Long)]]
              .map {
                tuple => tuple._2
              }

            println(s"sellerList: $sellerList")

            val priceUSD: List[BigDecimal] = vertex.getPropertyHistory("priceUSD").get
              .asInstanceOf[List[(Long, String)]]
              .map {tuple => tuple._2}
              .map(x => BigDecimal(x))

//            println(s"${
//              vertex.getPropertyOrElse("address",
//                vertex.getPropertyOrElse("nft", "unknown"))
//            }, priceUSD: $priceUSD")
//
//            println(s"step ${
//              vertex.getPropertyOrElse("address",
//                vertex.getPropertyOrElse("nft", "unknown"))
//            }, buyerList: $buyerList, sellerList: $sellerList")

            val collection = vertex.getPropertyOrElse("collection", "unknown")
            val category = vertex.getPropertyOrElse("category", "unknown")

            val zipBuyer = buyerList zip priceUSD
            val zipSeller = sellerList zip priceUSD

//            println(s"++++++buyer NFT ${
//              vertex.getPropertyOrElse("address",
//                vertex.getPropertyOrElse("nft", "unknown"))
//            }, test: $zipBuyer")
//
//            println(s"------sellr NFT ${
//              vertex.getPropertyOrElse("address",
//                vertex.getPropertyOrElse("nft", "unknown"))
//            }, test: $zipSeller")

            (buyerList zip priceUSD).foreach { tuple =>
              vertex.messageNeighbour(tuple._1, TraderInfo("buyer", 1, tuple._2))
              vertex.messageNeighbour(tuple._1, CollectionCategory("buyer", collection, category))
            }

            (sellerList zip priceUSD).foreach { tuple =>
              vertex.messageNeighbour(tuple._1, TraderInfo("seller", 1, tuple._2))
              vertex.messageNeighbour(tuple._1, CollectionCategory("seller", collection, category))
            }
          }


          if (vertex.getPropertyOrElse("address", otherwise = "nft") != "nft") {
            vertex.setState("purchased", 0)
            vertex.setState("sold", 0)
            vertex.setState("strength", 0)
            vertex.setState("amountSpent", 0)
            vertex.setState("amountGained", 0)
            vertex.setState("boughtNFTInfo", "")
            vertex.setState("soldNFTInfo", "")

            val activityDays = vertex.getPropertyHistory("nft").get
              .asInstanceOf[List[(Long, String)]]
              .map(tuple => tuple._1)
              .map(datetime => TimeUnit.DAYS.convert(datetime, TimeUnit.MILLISECONDS))
              .distinct
              .length
//            println(s"activityDays: $activityDays")
            vertex.setState("activityDays", activityDays)
          }

        }
      )
      .iterate({ vertex =>
        if (vertex.getPropertyOrElse("address", otherwise = "nft") != "nft") {

          val (traderInfo: List[TraderInfo], collCatInfo: List[CollectionCategory]) = vertex.messageQueue[Any]
            .partition {
              case _: TraderInfo => true
              case _ => false
            }

//          println(s"traderInfo: $traderInfo")
          println(s"collCatInfo: $collCatInfo")

          if (traderInfo.nonEmpty) {
            val queueMapTraderInfo = traderInfo
              .groupBy(traderInfo => traderInfo.toNode)
              .map(tup => (tup._1,
                tup._2.foldLeft(0)(_ + _.nTransaction),
                tup._2.foldLeft(BigDecimal(0))(_ + _.priceUSD))
              ).iterator.map(c => c._1 -> (c._2, c._3)).toMap

            println(s"*******queueMap: $queueMapTraderInfo")

            val amountSpent = queueMapTraderInfo.getOrElse("buyer", 0) match {
              case 0 => 0
              case x => x.asInstanceOf[(Int, BigDecimal)]._2
            }

            val amountGained = queueMapTraderInfo.getOrElse("seller", 0) match {
              case 0 => 0
              case x => x.asInstanceOf[(Int, BigDecimal)]._2
            }

            val purchased = queueMapTraderInfo.getOrElse("buyer", 0) match {
              case 0 => 0
              case x => x.asInstanceOf[(Int, Double)]._1
            }

            val sold = queueMapTraderInfo.getOrElse("seller", 0) match {
              case 0 => 0
              case x => x.asInstanceOf[(Int, Double)]._1
            }

            vertex.setState("purchased", purchased)
            vertex.setState("amountSpent", amountSpent)
            vertex.setState("sold", sold)
            vertex.setState("amountGained", amountGained)
            vertex.setState("strength", purchased + sold)
          }

          if (collCatInfo.nonEmpty) {
            val queueMapCollCategory = collCatInfo
              .groupBy(collcat => collcat.toNode)
              .map(tuple1 => (tuple1._1,
                tuple1._2
                  .groupBy(tuple2 => (tuple2.collection, tuple2.category))
                  .map(tuple3 => (tuple3._1, tuple3._2.length))
              ))
              .map(tuple4 => (tuple4._1, tuple4._2
                .map(tuple5 => (tuple5._1._1, tuple5._1._2, tuple5._2))))

            val boughtNFTInfo = queueMapCollCategory.getOrElse("buyer", "") match {
              case "" => ""
              case x:  List[(String, String, Int)] =>
                x.mkString(";")
                  .replace("_", "")
                  .replace(",", "_")
            }

            println(s"++++++++ ${queueMapCollCategory.getOrElse("buyer", "")}")
            println(s"boughtNFTInfo: $boughtNFTInfo")

            val soldNFTInfo = queueMapCollCategory.getOrElse("seller", "") match {
              case "" => ""
              case x:  List[(String, String, Int)] =>
                x.mkString(";")
                  .replace("_", "")
                  .replace(",", "_")
            }

            println(s"-------- ${queueMapCollCategory.getOrElse("seller", "")}")
            println(s"soldNFTInfo: $soldNFTInfo")

            vertex.setState("boughtNFTInfo", boughtNFTInfo)
            vertex.setState("soldNFTInfo", soldNFTInfo)

            println(s"queueMap: $queueMapCollCategory")
          }
        }
      }, iterations = 1, executeMessagedOnly = true)

      .select({ vertex =>
        if (vertex.getPropertyOrElse("address", otherwise = "nft") != "nft") {
          println(s" in select ${vertex.getPropertyOrElse("address",
            vertex.getPropertyOrElse("nft", "unknown"))}")

          Row(vertex.getPropertyOrElse("address", vertex.ID()),
            vertex.getStateOrElse("amountSpent", -1),
            vertex.getStateOrElse("amountGained", -1),
            vertex.getStateOrElse("purchased", -1),
            vertex.getStateOrElse("sold", -1),
            vertex.getStateOrElse("strength", -1),
            vertex.getStateOrElse("activityDays", -1),
            vertex.getStateOrElse("boughtNFTInfo", ""),
            vertex.getStateOrElse("soldNFTInfo", "")
          )

        } else Row()
        })
      .filter(row => row.getValues().length > 1)
      .writeTo(fileOutput)
  }
}

object TraderStrength {
  def apply(path: String="/Users/r160/TFM/tfm/nft-analysis/src/main/scala/com/raphtory/nft/output/raphtory/TraderStrength"):
  TraderStrength = {
    new TraderStrength(path + "TraderStrength")
  }
}








