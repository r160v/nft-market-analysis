package com.raphtory.nft.graphbuilders

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.model.graph._


class NFTGraphBuilder extends GraphBuilder[String] {
  val dtformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:m:ss")
  override def parseTuple(tuple: String): Unit = {
    val fields = tuple.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)").map(_.trim).toList

    val nft = fields(1)
    println(s"nft: $nft")
    val nftID = assignID(nft)
    val seller = fields(3)
    println(s"seller: $seller")
    val sellerID = assignID(seller)
    println(s"sellerID: $sellerID")
    val sellerUsername = fields(4)
    val buyer = fields(5)
    println(s"buyer: $buyer")
    val buyerID = assignID(buyer)
    println(s"buyerID: $buyerID")
    val buyerUsername = fields(6)
    val priceCrypto = fields(11)
    println(s"priceCrypto: $priceCrypto")
    val crypto = fields(12)
    println(s"crypto: $crypto")
    val priceUSD = fields(13) match {
      case "" => "0"
      case x => x
    }
    println(s"priceUSD: $priceUSD")
    val category = fields.last
    println(s"category: $category")
    val collection = fields(fields.length - 2)
    println(s"collection: $collection")
    val timestamp  = dtformat.parse(fields(fields.length - 5)).getTime
    println(s"timestamp: ${fields(fields.length - 5)}")
    println()

    /**
          We didn’t check whether either vertices exist before sending an addVertex update. Another class deals with
          this so we don’t have to worry about that.
     */

    // Add NFT node
    addVertex(timestamp, nftID, Properties(
      StringProperty("seller", seller),
      LongProperty("sellerID", sellerID),
      LongProperty("buyerID", buyerID),
      ImmutableProperty("nft", nft),
      StringProperty("priceUSD", priceUSD),
      StringProperty("crypto", crypto),
      StringProperty("priceCrypto", priceCrypto),
      ImmutableProperty("collection", collection),
      ImmutableProperty("category", category),
    ), Type("NFT"))

    println("test1")

    // Add Trader vertex for seller
    addVertex(timestamp, sellerID,
      Properties(
        StringProperty("nft", nft),
        ImmutableProperty("username", sellerUsername),
        ImmutableProperty("address", seller)
      ), Type("Trader"))

    println("test2")

    // Add Trader vertex for buyer
    addVertex(timestamp, buyerID,
      Properties(
        ImmutableProperty("address", buyer),
        ImmutableProperty("username", buyerUsername),
        StringProperty("nft", nft)
    ), Type("Trader"))

    println("test3")

    // Add NFT Transaction edge
    addEdge(timestamp, buyerID, sellerID, Type("NFT Transaction"))

    // Add NFT Ownership edge for buyer
    addEdge(timestamp, buyerID, nftID, Type("NFT Ownership"))

    // Remove NFT Ownership edge for seller (if exists)
    deleteEdge(timestamp, sellerID, nftID)
    println("end")

  }

}
