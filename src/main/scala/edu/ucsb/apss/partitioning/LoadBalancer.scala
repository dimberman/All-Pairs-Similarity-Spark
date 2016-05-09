package edu.ucsb.apss.partitioning

import org.apache.log4j.Logger

import scala.collection.mutable.{Set => MSet, Map => MMap}
import scala.util.control.Breaks


/**
  * Created by dimberman on 4/19/16.
  */


object MinOrder extends Ordering[(Int, Long)] {
    def compare(x: (Int, Long), y: (Int, Long)) = y._2 compare x._2
}


object LoadBalancer extends Serializable {
    type Key = (Int, Int)
    val stage2 = false

    def assignByIndex(bucket: Int, tiedLeader: Int, numBuckets: Int, neededVecs: Array[(Int, Int)]): List[(Int, Int)] = {
        numBuckets % 2 match {
            case 1 =>
                val start = neededVecs.indexOf((bucket, tiedLeader))
                val proposedBuckets = (List.range(start + 1, (start + 1) + (neededVecs.length - 1) / 2) :+ start)
                  .map(a => if (a < 0) a + neededVecs.length else a)
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


    def assignByBucket(bucket: Int, tiedLeader: Int, numBuckets: Int, neededVecs: Array[(Int, Int)]): List[(Int, Int)] = {
        numBuckets % 2 match {
            case 1 =>
                val start = bucket
                val proposedBuckets = (List.range(start + 1, (start + 1) + (numBuckets - 1) / 2)
                  :+ start
                  ).map(a => if (a < 0) a + numBuckets else a)
                val modded = proposedBuckets.map(a => a % numBuckets)


                val n = modded.flatMap(a => neededVecs.filter(b => b._1 == a))
                val ans = n.filter(c => isCandidate((bucket, tiedLeader), c))
                ans
            //                modded.map(neededVecs(_)).filter(isCandidate((bucket, tiedLeader), _)


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
        if (a._1 > b._1) {
            if (a._1 == a._2) true
            else !(a._2 >= b._1)
        }
            else if (a._1==b._1){
              a._2 >= b._2
        }
        else {
            if (b._1 == b._2) true
            //only compare if tiedleader(a) is < bucket(b)
            else !(b._2 >= a._1)
        }
    }


    /**
      * This is the load balancing algorithm introduced in _____
      */


    def balance(input: Map[Key, List[Key]], bucketSizes: Map[Key, Long], options: (Boolean, Boolean), log:Option[Logger] = None, debug:Boolean = false): Map[Key, List[Key]] = {
        val (s1, s2) = options

        val inp = MMap() ++ input.mapValues(MSet() ++ _.toSet).map(identity)


        val stage1 = if (s1) initialLoadAssignment(inp, bucketSizes) else inp
        handleLog("loadbalance: stage 1 complete", log)

        val balanced = if (s2) loadAssignmentRefinement(stage1, bucketSizes) else stage1
        handleLog("loadbalance: stage 2 complete", log)





        if(debug) {
            val initialCost = inp.map(calculateCost(_,bucketSizes)).values.toList
            handleLog(s"loadBalance: before balancing: std-dev = ${stdDev(initialCost)}",log)
        }

        if(debug){
            val s1cost = stage1.map(calculateCost(_,bucketSizes)).values.toList
            handleLog(s"loadBalance: after stage 1: std-dev = ${stdDev(s1cost)}",log)
        }

        if(debug){
            val s2cost = balanced.map(calculateCost(_,bucketSizes)).values.toList
            handleLog(s"loadBalance: after stage 1: std-dev = ${stdDev(s2cost)}",log)
        }

        balanced.mapValues(_.toList).toMap

//        input
    }


    def stdDev(s:List[Long])={
        val scores = s.map(_.toDouble)
        val count = scores.size
        val mean = scores.sum / count
        val devs = scores.map(score => (score - mean) * (score - mean))
        val stddev = Math.sqrt(devs.sum / count)
    }


    def handleLog(message:String, logger:Option[Logger]) = {
       logger match {
           case Some(x) =>
               x.info(s"breakdown: $message")
           case None =>
       }
    }


    def initialLoadAssignment(input: MMap[Key, MSet[Key]], bucketSizes: Map[Key, Long]): MMap[Key, MSet[Key]] = {
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


    def loadAssignmentRefinement(input: MMap[Key, MSet[Key]], bucketSizes: Map[Key, Long]): MMap[Key, MSet[Key]] = {
        var reduceable = true
        val outerLoop = new Breaks
        val innerLoop = new Breaks
        val nonReduceable = MSet[(Int,Int)]()
        var i = 0

        while (nonReduceable.size < input.size && i < 50000) {
            i += 1
            val costs = MMap() ++ input.map(calculateCost(_, bucketSizes))
            val orderedCosts = costs.filter{case(k,v) => !nonReduceable.contains(k)}.toList.sortBy(-_._2)
            val oldSize = nonReduceable.size
            innerLoop.breakable {
                orderedCosts.foreach {
                    case (k, v) =>
                        val c = bucketSizes(k)
                        var ic = v
                        val internalCosts = input(k).toList.map(x => (x, c * bucketSizes(x))).sortBy(_._2)
                        internalCosts.foreach {
                            case (f, h) =>
                                if (!nonReduceable.contains(f) && costs(f) + h < ic) {
                                    // if there is a switch that reduces std. div, perform switch and recalculate
                                    costs(f) = costs(f) + h
                                    ic -= h
                                    input(k) -= f
                                    input(f) += k
                                    innerLoop.break
                                }
                        }
                        //if there are no possible switches, this value is non-reduceable.
                        nonReduceable += k
                }
            }
        }
        input
    }


    def balanceByPartition(numPartitions: Int, balancedVectorMap: Map[Key, List[Key]], bucketSizes: Map[Key, Long]): Map[Key, Int] = {
        val inp = MMap() ++ balancedVectorMap.mapValues(MSet() ++ _.toSet).map(identity)
        val sortedByCost = inp.map(calculateCost(_, bucketSizes)).toList.sortBy(-_._2)
        val partitionMap: MMap[Int, Int] = MMap().withDefaultValue(0)
        val minHeap = scala.collection.mutable.PriorityQueue.empty(MinOrder)
        val partMap: MMap[Key, Int] = MMap[Key, Int]()
        for (i <- 0 to numPartitions) {
            minHeap.enqueue((i, 0))
        }
        sortedByCost.foreach {
            case (key, cost) =>
                val (lowestCostPartition, lowestCost) = minHeap.dequeue()
                partMap += (key -> lowestCostPartition)
                minHeap.enqueue((lowestCostPartition, lowestCost + cost))
        }
        partMap.toMap

    }



    def calculateCost(input: ((Key), MSet[Key]), bucketSizes: Map[Key, Long]): (Key, Long) = {
        val (key, values) = input

        (key, math.pow(bucketSizes(key), 2).toInt + values.map(
            k =>
                bucketSizes(key) * bucketSizes(k)
        ).sum)

    }

}
