package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.InvertedIndex.InvertedIndex._
import edu.ucsb.apss.InvertedIndex.InvertedIndex
import edu.ucsb.apss.{BucketMapping, VectorWithNorms}
import edu.ucsb.apss.partitioning.HoldensPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger

import scala.collection.mutable
import org.apache.log4j.{Level, Logger}

import org.apache.spark.Logging

import scala.collection.mutable.ArrayBuffer


/**
  * Created by dimberman on 1/3/16.
  */
class HoldensPSSDriver {
    val partitioner = new HoldensPartitioner
    //    @transient lazy val log = Logger.getLogger(getClass.getName)

    val log = Logger.getLogger(getClass.getName)

    def run(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double) = {
        val count = vectors.count

        val l1partitionedVectors = partitioner.partitionByL1Sort(vectors, numBuckets, count)
        val l = l1partitionedVectors.collect()
        //        val l1partitionedVectors = partitioner.partitionByL1GraySort(vectors, numBuckets, count).persist()
        //TODO this collect can be avoided if I can accesss values in partitioner
        val bucketLeaders = partitioner.determineBucketLeaders(l1partitionedVectors).collect().sortBy(_._1)
        val bucketizedVectors = partitioner.tieVectorsToHighestBuckets(l1partitionedVectors, bucketLeaders, threshold, sc).repartition(30)
        val invInd = bucketizedVectors.map {
            case ((ind, buck), v) => ((ind, buck), createFeaturePairs(v).toMap)
        }.reduceByKey { case (a, b) => mergeMap(a, b)((v1, v2) => v1 ++ v2) }
        //TODO would it be more efficient to do an aggregate?
        //        println(invInd.toDebugString)
        val invIndexes = invInd.mapValues(
            a => InvertedIndex(a)
        ).map { case (x, b) => ((x._1 * (x._1 + 1)) / 2 + x._2, (b, x)) }
        ////        val a = calculateCosineSimilarityUsingGroupByKey(l1partitionedVectors, invIndexes, assignments, thr//eshold)
        val a: RDD[(Long, Long, Double)] = calculateCosineSimilarityUsingCogroupAndFlatmap(bucketizedVectors, invIndexes, threshold, numBuckets)
        a
        //        a.map(_._2)


    }


    def pullKey(a: (Int, Int)) = (a._1 * (a._1 + 1)) / 2 + a._2

    def calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedVectors: RDD[((Int, Int), VectorWithNorms)], invIndexes: RDD[(Int, (InvertedIndex, (Int, Int)))], threshold: Double, numBuckets: Int): RDD[(Long, Long, Double)] = {

        //TODO test that this will guarantee that all key values will be placed into a single partition
        //TODO this function would be the perfect point to filter the values via static partitioning

        val par = partitioner.prepareTasksForParallelization(partitionedVectors, numBuckets)
        val parCount = par.countByKey().toList.sortBy(_._2)
        parCount.foreach{ case(idx, count) => log.info(s"partition $idx had $count vectors to calculate")}
//        val i = invIndexes.collect()
        val partitionedTasks: RDD[(Int, (Iterable[(Int, VectorWithNorms)], Iterable[(InvertedIndex, (Int, Int))]))] = par.cogroup(invIndexes).persist(StorageLevel.MEMORY_ONLY_SER)
        println(s"num partitions: ${partitionedTasks.partitions.length}")

        //        partitionedTasks.count()
        //                val a: RDD[(Long, Long, Double)] =
        val a: RDD[(Long, Long, Double)] = partitionedTasks.mapPartitions {
            iter =>
                iter.flatMap {

                    case (idx, (vectors, i)) =>
                        // there should only be one inverted index
                        //                        //TODO should I require 1 or would that take up a lot of time?
                        if (i.isEmpty) {

                            None
                        }

                        else {
                            val (inv, bucket) = i.head
                            val invertedIndex = inv.indices
                            val s1 = new Array[Double](invertedIndex.size)
                            val c = vectors.map {
                                case (buck, v) =>
                                    var r_j = v.l1
                                    val vec = v.vector
                                    val answer = ArrayBuffer[(Long, Long, Double)]()
                                    var i = 0

                                    //TODO you could probably just hold onto the indexes
                                    val fild_j = vec.indices.zipWithIndex.filter{case(x,y) => invertedIndex.contains(x)}
                                    val d_j = vec.indices
                                      .flatMap(
                                          ind =>
                                              if (invertedIndex.contains(ind)) {
                                                  i += 1
                                                  Some((ind, (v.index.toInt, vec.values(i - 1))))
                                              }
                                              else {
                                                  i += 1
                                                  None
                                              }
                                      )
                                    var k = 0



                                    d_j.foreach {
                                        case (featureIndex, (ind_j, weight_j)) =>
                                            var l = 0
                                            invertedIndex(featureIndex).foreach {
                                                case (featurePair) => {
                                                    val (ind_i, weight_i) = (featurePair.id, featurePair.weight)
                                                    if (!((s1(l) + v.lInf * r_j) < threshold))
                                                        s1(l) += weight_i * weight_j
                                                    l += 1
                                                }
                                                    r_j -= weight_j
                                            }
                                            l = 0
                                            invertedIndex(featureIndex).foreach {
                                                case (featurePair) => {
                                                    val (ind_i) = featurePair.id
                                                    if (s1(l) > threshold) {
                                                        val c = (ind_i, ind_j.toLong, s1(l))
                                                        answer += c

                                                    }
                                                    l += 1
                                                }
                                            }
                                    }


                                    //clear buffer
                                    d_j.foreach {
                                        case (feat, (ind_j, weight_j)) =>
                                            val a:Int =  invertedIndex(feat).length
                                            for(l:Int <- 0 to s1.length-1) {
                                                    s1(l) = 0
                                            }
                                    }
                                    answer.toList
                            }

                            c.flatten
                        }
                }

        }
        //        a.count()
        a
        //        a.map ( b => (1L, 2L, b))
        //
        //        a.map { case (g, b) => (g._1, g._2, b)}
        //        a
        //        val dog: RDD[(Long, Long, Double)] = partitionedVectors.context.parallelize(Seq((1L, 3L, 3.0)))
        //        dog
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


//class LogHolder extends Serializable {
//    @transient lazy val log = Logger.getLogger(getClass.getName)
//
//}
