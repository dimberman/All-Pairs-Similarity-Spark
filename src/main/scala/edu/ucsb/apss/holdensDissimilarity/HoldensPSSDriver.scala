package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.InvertedIndex.InvertedIndex._
import edu.ucsb.apss.InvertedIndex.InvertedIndex
import edu.ucsb.apss.{BucketMapping, VectorWithNorms}
import edu.ucsb.apss.partitioning.HoldensPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

import scala.collection.mutable


/**
  * Created by dimberman on 1/3/16.
  */
class HoldensPSSDriver {
    val partitioner = new HoldensPartitioner


    def run(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double) = {
        val count = vectors.count

        val partitionedVectors = partitioner.partitionByL1Sort(vectors, numBuckets, count).persist()
//        val partitionedVectors = partitioner.partitionByL1GraySort(vectors, numBuckets, count).persist()
        val bucketLeaders = partitioner.determineBucketLeaders(partitionedVectors).collect()
        val bucketizedVectors = partitioner.tieVectorsToHighestBuckets(partitionedVectors, bucketLeaders, threshold, sc)
        val invIndexes = bucketizedVectors.map {
            case ((ind, buck), v) => ((ind, buck), InvertedIndex(createFeaturePairs(v).toList)) }
          //TODO would it be more efficient to do an aggregate?
          .reduceByKey(
            mergeInvertedIndexes
        ).map { case (x, b) => ((x._1 * (x._1 + 1))/2 + x._2, (b, x)) }
        ////        val a = calculateCosineSimilarityUsingGroupByKey(partitionedVectors, invIndexes, assignments, thr//eshold)
        val a:RDD[(Int, (Long, Long, Double))] = calculateCosineSimilarityUsingCogroupAndFlatmap(bucketizedVectors, invIndexes, threshold, numBuckets)
        a.map(_._2)


    }


    def pullKey(a:(Int, Int)) = (a._1 * (a._1 + 1))/2 + a._2

    def calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedVectors: RDD[((Int, Int), VectorWithNorms)], invIndexes: RDD[(Int, (InvertedIndex,(Int,Int)))], threshold: Double, numBuckets:Int): RDD[(Int, (Long, Long, Double))] = {

        //TODO test that this will guarantee that all key values will be placed into a single partition
        //TODO this function would be the perfect point to filter the values via static partitioning

        val par = partitioner.prepareTasksForParallelization(partitionedVectors, numBuckets)
        val partitionedTasks:RDD[(Int, (Iterable[(Int,VectorWithNorms)], Iterable[(InvertedIndex, (Int, Int))]))] = par.cogroup(invIndexes)
        val a: RDD[(Int, (Long, Long, Double))] = partitionedTasks.flatMapValues {
            case (vectors, i) =>
                // there should only be one inverted index
                //TODO should I require 1 or would that take up a lot of time?
                if(i.isEmpty) None
                else {
                    val (inv, bucket) = i.head
                    val invertedIndex = inv.indices
                    val c = vectors.flatMap {
                        case (buck, v) =>
                            //                        val scores = new Array[Double](vectors.size)
                            val scores = new mutable.HashMap[(Long, Long), Double]() {
                                override def default(key: (Long, Long)) = 0
                            }
                            var r_j = v.l1
                            val vec = v.vector
                            val d_i = invertedIndex.filter(a => vec.indices.contains(a._1))
                            var i = 0
                            val d_j = vec.indices.flatMap(
                                ind =>
                                    if (d_i.contains(ind)) {
                                        i += 1
                                        Some((ind, (v.index.toInt, vec.values(i - 1))))
                                    }
                                    else {
                                        i += 1
                                        None
                                    }
                            )

                            val x = 5
                            d_j.foreach {
                                case (feat, (ind_j, weight_j)) =>
                                    d_i(feat).foreach {
                                        case (featurePair) => {
                                            val (ind_i, weight_i) = (featurePair.id, featurePair.weight)
                                            if (!((scores((ind_i, ind_j)) + v.lInf * r_j) < threshold))
                                                scores((ind_i, ind_j)) += weight_i * weight_j
                                        }
                                    }
                                    r_j -= weight_j
                            }
                            val s = scores.toList.filter(_._2 > threshold).map { case (g, b) => (g._1, g._2, b) }
                            s.toList
                    }
                    c
                }
        }
        a

    }


//    def calculateCosineSimilarityUsingGroupByKey(partitionedVectors: RDD[(Int, VectorWithNorms)], invIndexes: RDD[(Int, (InvertedIndex, Int))], assignments: List[BucketMapping], threshold: Double): RDD[(Int, (Int, Long, Double))] = {
//
//        //TODO test that this will guarantee that all key values will be placed into a single partition
//        //TODO this function would be the perfect point to filter the values via static partitioning
//
//        val partitionedTasks = partitioner.prepareTasksForParallelization(partitionedVectors, assignments).groupByKey().join(invIndexes)
//
//        val a = partitionedTasks.flatMapValues {
//            case (externalVectors, (invIndx, bucketID)) =>
//                val invertedIndex = invIndx.indices
//                externalVectors.flatMap {
//                    case (buck, v) =>
//                        val scores = new Array[Double](externalVectors.size)
//                        var r_j = v.l1
//                        val vec = v.vector
//                        val d_i = invertedIndex.filter(a => vec.indices.contains(a._1))
//                        var i = 0
//
//                        val d_j = vec.indices.flatMap(
//                            ind =>
//                                if (d_i.contains(ind)) {
//                                    i += 1
//                                    Some((ind, (v.index.toInt, vec.values(i - 1))))
//                                }
//                                else {
//                                    i += 1
//                                    None
//                                }
//                        )
//
//                        val x = 5
//                        d_j.foreach {
//                            case (feat, (ind_j, weight_j)) =>
//                                d_i(feat).foreach {
//                                    case (featurePair) => {
//                                        val (ind_i, weight_i) = (featurePair.id, featurePair.weight)
//                                        if (!((scores(v.index.toInt) + v.lInf * r_j) < threshold))
//                                            scores(ind_i) += weight_i * weight_j
//                                    }
//                                }
//                                r_j -= weight_j
//                        }
//                        val s = scores.zipWithIndex.filter(_._1 > threshold).map { case (score, ind_i) => (ind_i, v.index, score) }
//
//                        val a = 5
//                        s
//
//
//                }
//
//        }
//        a
//    }
}
