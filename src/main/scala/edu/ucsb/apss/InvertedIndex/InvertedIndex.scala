package edu.ucsb.apss.InvertedIndex

import java.io.{File, PrintWriter}


import edu.ucsb.apss.partitioning.{PartitionHasher, HoldensPartitioner}
import edu.ucsb.apss.preprocessing.TweetToVectorConverter
import edu.ucsb.apss.util.ExternalFileManager
import edu.ucsb.apss.util.PartitionUtil.VectorWithNorms
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


case class InvertedIndex(indices: Map[Int, List[FeaturePair]], bucket: Int = -1, tl: Int = -1, var splitIndex: Int = 0, var numSplits: Int = 1)

object InvertedIndex {
    type IndexMap = MMap[Int, List[FeaturePair]]
    type Bucket = (Int, Int)

    val log = Logger.getLogger(this.getClass)


    def generateInvertedIndexes(bucketizedVectors: RDD[((Int, Int), VectorWithNorms)], needsSplitting: Map[(Int, Int), Long] = Map(), numParts: Int): RDD[((Int, Int), InvertedIndex)] = {


        val incorrectAccum: Accumulator[ArrayBuffer[String]] = bucketizedVectors.context.accumulator(ArrayBuffer(""))(StringAccumulatorParam)
        val splitFeaturePairs: RDD[((Int, Int), (IndexMap, Bucket))] = extractInvertedIndex(bucketizedVectors)

        val mergedFeaturePairs = splitFeaturePairs.reduceByKey {
            case ((map1, idx1), (map2, idx2)) => {
                //                val (map1, idx1, map2, idx2) = (a._1, a._2, b._1, b._2)
                for (k <- map2.keys) {
                    if (map1.contains(k)) map1(k) = map1(k) ++ map2(k)
                    else map1 += (k -> map2(k))
                }
                if (idx1 != idx2) incorrectAccum += ArrayBuffer(s"index overlap shouldn't happen. values: $idx1, $idx2\n")
                //                require(idx1 == idx2, s"Values with different buckets have been given the same index. This shouldn't happen. values: $idx1, $idx2")
                (map1, idx1)
            }

        }

        incorrectAccum.value.tail.foreach(log.error(_))

        require(incorrectAccum.value.length < 2, "there were incorrectly partitioned inverted index values")

        mergedFeaturePairs.mapValues { case (a, (buck, tl)) => new InvertedIndex(a.toMap, buck, tl) }
    }


    def generateSplitInvertedIndexes(bucketizedVectors: RDD[((Int, Int), VectorWithNorms)], invSize: Int): RDD[((Int, Int), Iterable[SimpleInvertedIndex])] = {


        val incorrectAccum: Accumulator[ArrayBuffer[String]] = bucketizedVectors.context.accumulator(ArrayBuffer(""))(StringAccumulatorParam)




        val marked = bucketizedVectors.groupByKey().flatMap{
            case (x,i) =>
                i.zipWithIndex.map{case(j,k) => ((x,k/invSize),j)}
        }
        marked.groupByKey().map {
            case((k,v),i) =>
                (k, extractFeaturePairs(i.toList))

        }.groupByKey()



    }


    def splitInvertedIndexes(input: RDD[((Int, Int), InvertedIndex)], bMap: Map[(Int, Int), Long], numBuckets: Int): RDD[(Int, InvertedIndex)] = {
        val buckets = input.filter(_._2.indices.nonEmpty).keys.collect()
        val neededVecs = buckets.sortBy(a => a)


        val manager = new ExternalFileManager

        val pairs = buckets.map { case (b, t) => ((b, t), manager.assignByBucket(b, t, 0, neededVecs)) }
        val sums = pairs.toMap.mapValues(c => c.map(a => bMap(a)).filter(_ != -1).sum).map(identity)
        val average = sums.values.sum / sums.size
        val BVHasher = input.context.broadcast(new PartitionHasher)
        input.flatMap {
            case (i, v) => {
                val hash = BVHasher.value
                //                if (sums(i) < average) {
                v.splitIndex = 0
                v.numSplits = 1

                List((hash.partitionHash(i), v))
            }
        }

    }


    def extractFeaturePairs(vectors: List[VectorWithNorms]): SimpleInvertedIndex = {
        val featurePairs = vectors.map(createFeaturePairs)
        val featureMap = featurePairs.aggregate(MMap[Int, List[FeaturePair]]())(
            addFeaturePair,
            mergeFeatureMaps
        )

        val x =   featureMap.mapValues(a => a.toList).toMap

        SimpleInvertedIndex(x)
    }

    def addFeaturePair(a: MMap[Int, List[FeaturePair]], b: Array[(Int, List[FeaturePair])]): MMap[Int, List[FeaturePair]] = {
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


    def createFeaturePairs(vector: VectorWithNorms): Array[(Int, List[FeaturePair])] = {
        vector.vector.indices.map(i => (i, List(FeaturePair(vector.index, vector.vector(i)))))
    }

    def apply(a: VectorWithNorms): InvertedIndex = {
        new InvertedIndex(createFeaturePairs(a).toMap)
    }

    def apply(a: List[(Int, List[FeaturePair])], buck: Int, tl: Int) = new InvertedIndex(a.toMap, buck, tl)

    def apply(a: List[(Int, List[FeaturePair])]) = new InvertedIndex(a.toMap)


    def apply() = {
        new InvertedIndex(Map())
    }

    def extractIndexMap(i: InvertedIndex): Map[Long, Int] = {
        i.indices.values.map(a => a.map(_.id)).reduce(_ ++ _).distinct.zipWithIndex.toMap
    }


    def extractIndexMaFromSimple(i: SimpleInvertedIndex): Map[Long, Int] = {
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