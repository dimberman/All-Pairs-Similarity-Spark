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
        r.map(v => (l1Norm(v.vec), v)).sortByKey().zipWithIndex().map { case ((l1, vec), sortIndex) => ((sortIndex.toFloat / numVectors.toFloat * numBuckets).toInt, vec) }
    }


    def determineBucketLeaders(r: RDD[(Int, VectorWithNorms)]): Array[(Int, Double)] = {
        val answer = r.map { case (k, v) => (k, v.l1) }.reduceByKey((a, b) => math.max(a, b)).collect().sortBy(_._1)
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


    def tieVectorsToHighestBuckets(inputVectors: RDD[(Int, VectorWithNorms)], leaders: Array[(Int, Double)], threshold: Double, sc: SparkContext): RDD[((Int, Int), VectorWithNorms)] = {
        //this step should reduce the amount of data that needs to be shuffled
        val idealVectors = determineIdealVectors(inputVectors)
//        idealVectors.foreach(a => println(s"ideal vectors: $a"))
        val persistedInputvecs = inputVectors.persist()
        val lInfNormsOnly = persistedInputvecs.mapValues(_.lInf)
        val buckets: RDD[Int] = lInfNormsOnly.mapPartitions {
            case (i) =>
                val idealMap:MMap[(Int,Int), Double] = MMap()

                i.map {
                    case (bucket, infNorm) =>
                        val tmax = threshold / infNorm
                        var res = 0
                        if (tmax < leaders.head._2) res = bucket + 1
                        else if (bucket == 0) res = 1
                        else {
                            while (res < bucket && tmax > leaders(res)._2) {
                                res += 1
                            }
                            while (res < bucket && getMaximalSimilarity((bucket,res),idealVectors(bucket)._2, idealVectors(res)._2, idealMap) < threshold) {
                                res += 1
                            }
                        }
                        res - 1
                }
        }



        val ret = persistedInputvecs.zip(buckets).map {
            case ((key, vec), matchedBucket) =>
                //TODO mutable values would be faster
                val nVec = new VectorWithNorms(vec.lInf, vec.l1, vec.normalizer, vec.vector, vec.index, matchedBucket)
                ((key, matchedBucket), nVec)
        }
        persistedInputvecs.unpersist()
        ret
    }

    def getMaximalSimilarity(key:(Int,Int), vectorA:SparseVector, vectorB:SparseVector, idealMap:MMap[(Int,Int), Double]):Double = {
        if(idealMap.contains(key)) idealMap(key)
        else{
            val ans = PartitionUtil.dotProduct(vectorA, vectorB)
            idealMap+=(key -> ans)
            ans
        }
    }

}








