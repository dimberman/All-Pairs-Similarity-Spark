package edu.ucsb.apss.partitioning

import edu.ucsb.apss.Context
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import edu.ucsb.apss.util.PartitionUtil
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import scala.io.Source

/**
  * Created by dimberman on 5/14/16.
  */
class CluewebDebugTest extends FlatSpec with Matchers with BeforeAndAfter {
    val clueweb = Context.sc.parallelize(Source.createBufferedSource(getClass.getResourceAsStream("10k-clueweb")).getLines.toList)
    "convertTweets" should "convert clueweb into super large vectors" in {
        val t = new TweetToVectorConverter
        val x = clueweb.map(x => (x,t.convertTweetToVector(x))).mapValues(PartitionUtil.normalizeVector)
          .map{case (s,v) => (PartitionUtil.lInfNorm(v),(s,v))}
          .reduce{case((a,av),(b,bv)) => if(a > b) (a,av) else (b,bv)}._2

        val topString =  x._1
        val y = topString.split(" ").toList
          .filter(s => s contains "motorsports")
        printBlah(topString)

        println()
        println()
        println()
        println()

        val topVec = t.convertTweetToVector(topString)
        val hiV = topVec.values.zipWithIndex.filter(_._1 > 100)
        val w = topVec.values.sum
        printBlah(topVec.toString())

//        val normalizedTx/opVec


    }


    def printBlah(s:String) = {
        s.grouped(200).toList.foreach(println)
    }

}
