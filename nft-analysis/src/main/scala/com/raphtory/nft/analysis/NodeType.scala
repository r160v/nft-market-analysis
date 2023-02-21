package com.raphtory.nft.analysis

abstract class NodeType

object NodeType {
  case object Trader extends NodeType
  case object NFT extends NodeType
  case object TraderNFT extends NodeType
}
