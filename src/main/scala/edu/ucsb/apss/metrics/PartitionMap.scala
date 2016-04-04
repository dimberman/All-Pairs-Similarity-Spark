package edu.ucsb.apss.metrics
import scala.collection.mutable.{Map => MMap}

/**
  * Created by dimberman on 3/22/16.
  */
//class PartitionMap {
//     private val pMap = MMap[String,Long]()
//
//
//    def put(a:(Int,Int), b:Long) = {
//        put(a._1, a._2, b)
//    }
//
//    def put(a:Int, b:Int, c:Long):Boolean = {
//        pMap += (s"${a}_$b" -> c)
//        true
//    }
//
//    def get(a:Int, b:Int):Long = {
//        pMap.get(s"${a}_$b").get
//    }
//
//    def get(a:(Int,Int)):Long = {
//        get(a._1, a._2)
//    }
//
//    def countSkipped(bucketSize:Int) = {
//        var i = 0
//        pMap.foreach{
//            case(key,num) =>
//
//
//        }
//    }
//
//}
