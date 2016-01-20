package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.InvertedIndex.InvertedIndex._
import edu.ucsb.apss.InvertedIndex.InvertedIndex
import edu.ucsb.apss.{BucketMapping, VectorWithNorms}
import edu.ucsb.apss.partitioning.HoldensPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD


/**
  * Created by dimberman on 1/3/16.
  */
class HoldensPSSDriver {
    val partitioner = new HoldensPartitioner


    def run(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double) = {
        val count = vectors.count

        val partitionedVectors = partitioner.partitionByL1Norm(vectors, numBuckets, count).persist()

        val bucketLeaders = partitioner.determineBucketLeaders(partitionedVectors).collect()

        //TODO should I modify this so that it uses immutable objects?
        partitioner.tieVectorsToHighestBuckets(partitionedVectors, bucketLeaders, threshold, sc)

        val invIndexes = partitionedVectors.map { case (ind, v) => (ind, InvertedIndex(createFeaturePairs(ind, v).toList)) }
          //TODO would it be more efficient to do an aggregate?
          .reduceByKey(
            mergeInvertedIndexes
        ).map { case (x, b) => (x, (b, x)) }


        val assignments = partitioner.createPartitioningAssignments(numBuckets)

        //TODO test that this will guarantee that all key values will be placed into a single partition
        //TODO this function would be the perfect point to filter the values via static partitioning


//        val a = calculateCosineSimilarityUsingGroupByKey(partitionedVectors, invIndexes, assignments, threshold)
        val a:RDD[(Int, (Int, Long, Double))] = calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedVectors, invIndexes, assignments, threshold)
        a.map(_._2)


    }


    def calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedVectors: RDD[(Int, VectorWithNorms)], invIndexes: RDD[(Int, (InvertedIndex, Int))], assignments: List[BucketMapping], threshold: Double): RDD[(Int, (Int, Long, Double))] = {
        val partitionedTasks = partitioner.prepareTasksForParallelization(partitionedVectors, assignments).cogroup(invIndexes)

        val a: RDD[(Int, (Int, Long, Double))] = partitionedTasks.flatMapValues {
            case (vectors, i) =>
                val (inv, bucket) = i.head

                val invertedIndex = inv.indices
                val c = vectors.flatMap {
                    case (buck, v) =>
                        val scores = new Array[Double](vectors.size)
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
                                        if (!((scores(v.index.toInt) + v.lInf * r_j) < threshold))
                                            scores(ind_i) += weight_i * weight_j
                                    }
                                }
                                r_j -= weight_j
                        }
                        val s = scores.zipWithIndex.filter(_._1 > threshold).map { case (score, ind_i) => (ind_i, v.index, score) }
                        s.toList
                }
                c
        }
        a

    }


    def calculateCosineSimilarityUsingGroupByKey(partitionedVectors: RDD[(Int, VectorWithNorms)], invIndexes: RDD[(Int, (InvertedIndex, Int))], assignments: List[BucketMapping], threshold: Double): RDD[(Int, (Int, Long, Double))] = {


        val partitionedTasks = partitioner.prepareTasksForParallelization(partitionedVectors, assignments).groupByKey().join(invIndexes)

        val a = partitionedTasks.flatMapValues {
            case (externalVectors, (invIndx, bucketID)) =>
                val invertedIndex = invIndx.indices
                externalVectors.flatMap {
                    case (buck, v) =>
                        val scores = new Array[Double](externalVectors.size)
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
                                        if (!((scores(v.index.toInt) + v.lInf * r_j) < threshold))
                                            scores(ind_i) += weight_i * weight_j
                                    }
                                }
                                r_j -= weight_j
                        }
                        val s = scores.zipWithIndex.filter(_._1 > threshold).map { case (score, ind_i) => (ind_i, v.index, score) }

                        val a = 5
                        s


                }

        }
        a
    }
}
