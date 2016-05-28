package edu.ucsb.apss.partitioning

import edu.ucsb.apss.util.{VectorWithNorms, PartitionUtil, VectorWithIndex}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Map => MMap}
import java.util


/**
  * Created by dimberman on 12/10/15.
  */


object StaticPartitioner extends Serializable {
    def l1Norm(v: SparseVector) = {
        v.values.map(math.abs).sum
    }


    def lInfNorm(v: SparseVector) = {
        v.values.map(math.abs).max
    }

    def normalizer(v: SparseVector) = {
        val a = v.values.map(a => a * a).sum
        math.sqrt(a)
    }


    def normalizeVectors(vecs: RDD[SparseVector]): RDD[SparseVector] = {
        vecs.map {
            vec =>
                val normalizer = StaticPartitioner.normalizer(vec)
                for (i <- vec.values.indices) vec.values(i) = vec.values(i) / normalizer
                new SparseVector(vec.size, vec.indices, vec.values)
        }

    }


    def recordIndex(r: RDD[SparseVector]): RDD[VectorWithIndex] = {
        r.zipWithIndex().map { case (vec, ind) => VectorWithIndex(vec, ind) }
    }


    def partitionByL1Sort(r: RDD[VectorWithIndex], numBuckets: Int, numVectors: Long): RDD[(Int, VectorWithIndex)] = {
        r.map(v => (l1Norm(v.vec), v))
          .sortByKey()
          .zipWithIndex()
          .map {
              case ((l1, vec), sortIndex) =>
                  ((sortIndex.toFloat / numVectors.toFloat * numBuckets).toInt, vec)
          }
    }


    def determineBucketLeaders(r: RDD[(Int, VectorWithNorms)]): Array[(Int, Double)] = {
        val answer = r.map { case (k, v) => (k, v.l1) }.reduceByKey((a, b) => math.max(a, b)).collect().sortBy(_._1)
        answer
    }

    def determineBucketMaxes(r: RDD[(Int, VectorWithNorms)]): Array[(Int, Double)] = {
        val answer = r.map { case (k, v) => (k, v.lInf) }.reduceByKey((a, b) => math.max(a, b)).collect().sortBy(_._1)
        answer
    }


    def determineIdealVectors(r: RDD[(Int, VectorWithNorms)]): Array[(Int, SparseVector)] = {
        r.aggregateByKey(MMap[Int, Double]())(addVector, mergeMaps)
          .mapValues(map => new SparseVector(map.size, map.keys.toArray, map.values.toArray))
          .collect()
    }

    def addVector(a: MMap[Int, Double], b: VectorWithNorms) = {
        val vec = b.vector
        for (i <- vec.indices.indices) {
            if (a.contains(vec.indices(i)))
                a(vec.indices(i)) = math.max(a(vec.indices(i)), vec.values(i))
            else a += (vec.indices(i) -> vec.values(i))
        }
        a
    }

    def mergeMaps(a: MMap[Int, Double], b: MMap[Int, Double]) = {
        for (key <- b.keys) {
            if (a.contains(key)) a(key) = math.max(a(key), b(key))
            else a += (key -> b(key))
        }
        a
    }


    def tieVectorsToHighestBuckets(inputVectors: RDD[(Int, VectorWithNorms)], sumLeaders: Array[(Int, Double)], maxLeaders: Array[(Int, Double)], threshold: Double, sc: SparkContext): RDD[((Int, Int), VectorWithNorms)] = {
        val idealVectors = determineIdealVectors(inputVectors).toMap

        inputVectors.mapPartitions {
            iter =>
                val idealMap: MMap[(Int, Int), Double] = MMap()
                iter.map {
                    case (bucket, vec) =>
                        val infNorm = vec.lInf
                        val l1norm = vec.l1

                        val tMax = threshold / infNorm
                        val tSum = threshold / l1norm

                        var res = 0
                        while (
                            tMax > sumLeaders(res)._2
//                              ||
//                              tSum > maxLeaders(res)._2 ||
//                              threshold / maxLeaders(res)._2 > l1norm ||
//                              threshold / sumLeaders(res)._2 > vec.lInf
                        ) {
                            res += 1
                        }
                        //                            while (res < bucket && getMaximalSimilarity((bucket,res),idealVectors(bucket), idealVectors(res), idealMap) < threshold) {
                        //                                res += 1
                        //                            }
                        //                            if(getMaximalSimilarity((bucket,res),idealVectors(bucket), idealVectors(res-1), idealMap) > threshold && bucket != (res-1))
                        //                                println(s"maximal ideal: ($bucket, $res) " + getMaximalSimilarity((bucket,res),idealVectors(bucket), idealVectors(res), idealMap))
                        res -= 1
                        if(res < 0) res = bucket
                        val ans = res
                        require(ans != -1, "something went wrong and there is a bucket that was never given a tl")
                        vec.associatedLeader = ans
                        ((bucket, ans), vec)
                }
        }
    }


    def getMaximalSimilarity(key: (Int, Int), vectorA: SparseVector, vectorB: SparseVector, idealMap: MMap[(Int, Int), Double]): Double = {
        if (idealMap.contains(key)) idealMap(key)
        else {
            val ans = PartitionUtil.dotProduct(vectorA, vectorB)
            idealMap += (key -> ans)
            ans
        }
    }

}








