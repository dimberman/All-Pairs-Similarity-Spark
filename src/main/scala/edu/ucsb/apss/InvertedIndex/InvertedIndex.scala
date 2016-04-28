package edu.ucsb.apss.InvertedIndex

import java.io.{File, PrintWriter}


import edu.ucsb.apss.partitioning.{LoadBalancer, PartitionHasher, StaticPartitioner}
import edu.ucsb.apss.util.{VectorWithNorms, FileSystemManager}
import org.apache.log4j.Logger
import org.apache.spark.{AccumulatorParam, Accumulator, SparkConf, SparkContext}
import scala.collection.mutable.{Map => MMap, ArrayBuffer}

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

import scala.util.Random


/**
  * Created by dimberman on 1/14/16.
  */
case class BucketHolder(b: (Int, Int)) extends Serializable


case class SimpleInvertedIndex(indices: Map[Int, List[FeaturePair]])

object InvertedIndex {
    type IndexMap = MMap[Int, List[FeaturePair]]
    type Bucket = (Int, Int)

    val log = Logger.getLogger(this.getClass)


    def createFeaturePairs(vectorWithNorms: VectorWithNorms): Array[(Int, List[FeaturePair])] = {
        val VectorWithNorms(_,_,_,vec, index,_) = vectorWithNorms
        vec.indices.indices.map(i => (vec.indices(i), List(FeaturePair(index, vec.values(i))))).toArray
    }

    def generateSplitInvertedIndexes(bucketizedVectors: RDD[((Int, Int), VectorWithNorms)], invSize: Int): RDD[((Int, Int), Iterable[SimpleInvertedIndex])] = {


//        val incorrectAccum: Accumulator[ArrayBuffer[String]] = bucketizedVectors.context.accumulator(ArrayBuffer(""))(StringAccumulatorParam)




        val marked = bucketizedVectors.groupByKey().flatMap{
            case (x,i) =>
                i.zipWithIndex.map{case(j,k) => ((x,k/invSize),j)}
        }
        marked.groupByKey().map {
            case((k,v),i) =>
                (k, extractFeaturePairs(i.toList))

        }.groupByKey()



    }



    def extractFeaturePairs(vectors: List[VectorWithNorms]): SimpleInvertedIndex = {
        val featurePairs = vectors.map(createFeaturePairs)
        val featureMap = featurePairs.aggregate(MMap[Int, List[FeaturePair]]())(
            addFeaturePairsToMap,
            mergeFeatureMaps
        )

        val x =   featureMap.mapValues(a => a.toList).toMap

        SimpleInvertedIndex(x)
    }

    def addFeaturePairsToMap(a: MMap[Int, List[FeaturePair]], b: Array[(Int, List[FeaturePair])]): MMap[Int, List[FeaturePair]] = {
        b.foreach {
            case (k, v) =>
                if (a.contains(k)) {
                    a(k) = a(k) ++ v
                }
                else a(k) = v
        }
        a


    }


    def mergeFeatureMaps(a: MMap[Int, List[FeaturePair]], b: MMap[Int, List[FeaturePair]]): MMap[Int, List[FeaturePair]] = {
        for (k <- b.keys) {
            if (a.contains(k)) {
                a(k) = a(k) ++ b(k)
            }
            else {
                a(k) = b(k)
            }
        }
        a
    }


    def extractInvertedIndex(bucketizedVectors: RDD[((Int, Int), VectorWithNorms)]): RDD[((Int, Int), (IndexMap, Bucket))] = {
        bucketizedVectors.map {
            case (x, v) => {
                val featureMap: IndexMap = MMap[Int, List[FeaturePair]]() ++= createFeaturePairs(v).toMap
                (x, (featureMap, x))
            }
        }
    }

    def mergeFeaturePairs(a: (IndexMap, Bucket), b: (IndexMap, Bucket)): (IndexMap, Bucket) = {
        val (map1, idx1, map2, idx2) = (a._1, a._2, b._1, b._2)
        for (k <- map2.keys) {
            if (map1.contains(k)) map1(k) = map1(k) ++ map2(k)
            else map1 += (k -> map2(k))
        }
        require(idx1 == idx2, s"Values with different buckets have been given the same index. This shouldn't happen. values: $idx1, $idx2")
        (map1, idx1)
    }





    def extractIndexMapFromSimple(i: SimpleInvertedIndex): Map[Long, Int] = {
        i.indices.values.map(a => a.map(_.id)).reduce(_ ++ _).distinct.zipWithIndex.toMap
    }
}


case class FeaturePair(id: Long, weight: Double)


object StringAccumulatorParam extends AccumulatorParam[ArrayBuffer[String]] {

    def zero(initialValue: ArrayBuffer[String]): ArrayBuffer[String] = {
        ArrayBuffer("")
    }

    def addInPlace(s1: ArrayBuffer[String], s2: ArrayBuffer[String]): ArrayBuffer[String] = {
        if (s1.length + s2.length < 2000)
            s1 ++ s2
        else s1
    }
}