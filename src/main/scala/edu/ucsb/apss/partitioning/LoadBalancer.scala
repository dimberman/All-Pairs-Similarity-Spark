package edu.ucsb.apss.partitioning

import scala.collection.mutable.{Set => MSet, Map => MMap, ListBuffer, ArrayBuffer}

/**
  * Created by dimberman on 4/19/16.
  */


object LoadBalancer extends Serializable {
    type Key = (Int, Int)

    def assignByBucket(bucket: Int, tiedLeader: Int, numBuckets: Int, neededVecs: Array[(Int, Int)]): List[(Int, Int)] = {
        numBuckets % 2 match {
            case 1 =>
                val start = neededVecs.indexOf((bucket, tiedLeader))
                //                val proposedBuckets = List.range(bucket + 1, (bucket + 1) + (numBuckets - 1) / 2) :+ bucket
                val proposedBuckets = (List.range(start + 1, (start + 1) + (start - 1) / 2).map(_ - neededVecs.length / 2) :+ start).map(a => if (a < 0) a + neededVecs.length else a)
                val modded = proposedBuckets.map(a => a % neededVecs.size)
                modded.map(neededVecs(_)).filter(isCandidate((bucket, tiedLeader), _)

                )
            case 0 =>
                if (bucket < numBuckets / 2) {
                    val e = List.range(bucket + 1, (bucket + 1) + numBuckets / 2).map(_ % numBuckets) :+ bucket
                    val modded = e.map(a => a % neededVecs.size)
                    modded.map(neededVecs(_)).filter(isCandidate((bucket, tiedLeader), _))
                }
                else {
                    val x = (bucket + 1) + numBuckets / 2 - 1
                    val e = List.range(bucket + 1, x).map(_ % numBuckets) :+ bucket
                    val modded = e.map(a => a % neededVecs.size)
                    modded.map(neededVecs(_)).filter(isCandidate((bucket, tiedLeader), _))
                }
        }

    }


    def isCandidate(a: (Int, Int), b: (Int, Int)): Boolean = {
        if (a._1 == a._2 || b._1 == b._2) true
        else !((a._2 >= b._1) || (b._2 >= a._1))
        //        true
    }


    /**
      * This is the load balancing algorithm introduced in _____
      */



    def balance(input: Map[Key, List[Key]], bucketSizes: Map[Key, Int]):Map[Key, List[Key]] = {
        val inp = MMap() ++ input.mapValues(MSet() ++ _.toSet).map(identity)
        balance(inp, bucketSizes).mapValues(_.toList).toMap

    }


    def balance(input: MMap[Key, MSet[Key]], bucketSizes: Map[Key, Int]):MMap[Key, MSet[Key]] = {
        val answer: MMap[Key, MSet[Key]] = MMap()
        while (input.nonEmpty) {
            val costMap = input.map(calculateCost(_, bucketSizes))
            val minVal = costMap.par.reduce((a, b) => if (a._2 < b._2) a else b)._1
            answer += (minVal -> input(minVal))
            input -= minVal
            input.foreach {
                case (k, v) =>
                    if (v.contains(minVal)) {
                        answer(minVal) += k
                        v -= minVal
                    }
            }
        }
        answer
    }


    def calculateCost(input: ((Key), MSet[Key]), bucketSizes: Map[Key, Int]): (Key, Int) = {
        val (key, values) = input

        (key, math.pow(bucketSizes(key), 2).toInt + values.map(
            k =>
                bucketSizes(key) * bucketSizes(k)
        ).sum)

    }

}
