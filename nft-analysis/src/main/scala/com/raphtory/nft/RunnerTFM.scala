package com.raphtory.nft

import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.nft.analysis.{EdgeWeight, NFT, NodeType, TraderNFTDegree, TraderStrength}
import com.raphtory.nft.graphbuilders.{NFTGraphBuilder, NFTGraphBuilderCleanDataset}
import com.raphtory.nft.spouts.NFTSpout

import java.util.Date
import java.util.concurrent.TimeUnit

object RunnerTFM extends App {
        val source  = new NFTSpout("src/main/scala/com/raphtory/nft/data/","testAlgorithms.csv")  // EdgeWeight/TraderStrength
//        val source  = new NFTSpout("src/main/scala/com/raphtory/nft/data/","testAlgorithmCleaned.csv")  // EdgeWeight/TraderStrength

        val builder = new NFTGraphBuilder()
//        val builder = new NFTGraphBuilderCleanDataset()
        val rg = RaphtoryGraph[String](source,builder)

        val dtformat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:m:ss")

        val testDate = 1574449173000L

        val outputPath = "/Users/r160/TFM/tfm/nft-analysis/src/main/scala/com/raphtory/nft/output/raphtory/"


//        println(s"$testDate: ${dtformat.format(new Date(testDate))}")

//        println(s"${dtformat.parse("2019-11-20 17:59:33").getTime}")
//        println(s"${dtformat.parse("2019-11-20 15:59:33").getTime}")
//        println(s"${dtformat.parse("2019-11-20 20:59:33").getTime}")
//        println(s"${dtformat.parse("2019-11-21 23:59:33").getTime}")
//        println(s"${dtformat.parse("2019-11-22 19:59:33").getTime}")
//        println(s"${dtformat.parse("2019-12-01 23:59:33").getTime}")
//        println(s"${dtformat.parse("2019-12-05 23:59:33").getTime}")
//        println(s"${dtformat.parse("2019-12-06 23:59:33").getTime}")
//        println(s"${dtformat.parse("2019-12-10 23:59:33").getTime}")

        val startTime = dtformat.parse("2019-11-19 00:00:00").getTime
        val timestamp_analysis = dtformat.parse("2019-12-20 00:00:00").getTime
        val endTime = timestamp_analysis

        // the end of the window lines up with the point of interest
        //val increment = TimeUnit.DAYS.toMillis(1)
        //val windows = TimeUnit.DAYS.toMillis(7)
        val increment = TimeUnit.DAYS.toMillis(1)
        val windows = TimeUnit.DAYS.toMillis(2)
//        val increment = TimeUnit.HOURS.toMillis(1)
//        val windows = TimeUnit.HOURS.toMillis(1)

        //val date = new Date(timestamp_analysis)
        //println(s"backto date : ${dtformat.format(timestamp_analysis)}")
        //val test = TimeUnit.DAYS.convert(timestamp_analysis, TimeUnit.MILLISECONDS);
        //println(test)

        println(s"Last time: $timestamp_analysis")
//        rg.pointQuery(TraderNFTDegree(outputPath, vertexType = NodeType.NFT), timestamp = timestamp_analysis)  // OK
//        rg.rangeQuery(TraderNFTDegree(outputPath, vertexType = NodeType.NFT), start = startTime, end = endTime, increment = increment, windows = List(windows))  // OK
//        rg.pointQuery(TraderNFTDegree(outputPath, vertexType = NodeType.Trader), timestamp = timestamp_analysis)  // OK
//        rg.rangeQuery(TraderNFTDegree(outputPath, vertexType = NodeType.Trader), start = startTime, end = endTime, increment = increment, windows = List(windows))  // OK
//        rg.pointQuery(EdgeWeight(outputPath), timestamp = timestamp_analysis)  // OK
//        rg.rangeQuery(EdgeWeight(outputPath), start = startTime, end = endTime, increment = increment, windows = List(windows))  // OK
//        rg.pointQuery(TraderStrength(outputPath), timestamp = timestamp_analysis)  // OK
        rg.rangeQuery(TraderStrength(outputPath), start = startTime, end = endTime, increment = increment, windows = List(windows))  //
//        rg.rangeQuery(NFT(outputPath), start = startTime, end = endTime, increment = TimeUnit.MINUTES.toMillis(1), windows = List(TimeUnit.MINUTES.toMillis(1)))  //

}
        //1575154773000
        //1575154773000
        //println(dtformat.parse("2019-11-30 23:59:33").getTime) //1575154773000
        //println(dtformat.parse("2019-12-01 23:00:01").getTime) //1575237601000
        //println(dtformat.parse("2019-12-01 23:59:33").getTime) //1575241173000
        //rg.rangeQuery(new ConnectedComponents(Array()), serialiser = new DefaultSerialiser, start=1, end = 32674, increment=1000,windowBatch=List(10000, 1000,100))
        //rg.rangeQuery(ConnectedComponents(), serialiser = new DefaultSerialiser,start = 1,end = 32674,increment = 100,arguments)
        //rg.rangeQuery(ConnectedComponents(), serialiser = new DefaultSerialiser,start = 1,end = 32674,increment = 100,window=100,arguments)
        //rg.rangeQuery(ConnectedComponents(), serialiser = new DefaultSerialiser,start = 1,end = 32674,increment = 100,windowBatch=List(100,50,10),arguments)
        //rg.viewQuery(DegreeBasic(), serialiser = new DefaultSerialiser,timestamp = 10000,arguments)
        //rg.viewQuery(DegreeBasic(), serialiser = new DefaultSerialiser,timestamp = 10000,window=100,arguments)
