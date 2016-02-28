package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.InvertedIndex.InvertedIndex._
import edu.ucsb.apss.InvertedIndex.InvertedIndex
import edu.ucsb.apss.VectorWithNorms
import edu.ucsb.apss.partitioning.HoldensPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger


/**
  * Created by dimberman on 1/3/16.
  */
class HoldensPSSDriver {
    val partitioner = new HoldensPartitioner
    //    @transient lazy val log = Logger.getLogger(getClass.getName)

    val log = Logger.getLogger(getClass.getName)

    type BucketizedVector = ((Int, Int), VectorWithNorms)


    def bucketizeVectors(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double): RDD[((Int, Int), VectorWithNorms)] = {
        val count = vectors.count
        val l1partitionedVectors = partitioner.partitionByL1Sort(vectors, numBuckets, count)
        val bucketLeaders = partitioner.determineBucketLeaders(l1partitionedVectors).collect().sortBy(_._1)
        partitioner.tieVectorsToHighestBuckets(l1partitionedVectors, bucketLeaders, threshold, sc)
    }


    def run(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double) = {
        val bucketizedVectors: RDD[BucketizedVector] = bucketizeVectors(sc, vectors, numBuckets, threshold).repartition(15)

        val numParts = (numBuckets * (numBuckets + 1)) / 2

        val needsSplitting = bucketizedVectors.countByKey().filter(_._2 > 2500).map { case ((a, b), c) => ((a.toInt, b.toInt), c) }.toMap

        val invertedIndexes = generateInvertedIndexes(bucketizedVectors, needsSplitting, numParts)
        val invIndexSums = generateInvertedIndexes(bucketizedVectors, needsSplitting, numParts)

        val partitionedTasks = pairVectorsWithInvertedIndex(bucketizedVectors, invertedIndexes, numBuckets, needsSplitting).persist(StorageLevel.MEMORY_AND_DISK_SER)

        val a: RDD[(Long, Long, Double)] = calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedTasks, threshold, numBuckets)



        a

    }


    def pullKey(a: (Int, Int)) = (a._1 * (a._1 + 1)) / 2 + a._2


    def pairVectorsWithInvertedIndex(partitionedVectors: RDD[((Int, Int), VectorWithNorms)], invIndexes: RDD[(Int, (InvertedIndex))], numBuckets: Int, needsSplitting: Map[(Int, Int), Long]): RDD[(Int, (Iterable[(Int, VectorWithNorms)], Iterable[InvertedIndex]))] = {
        val neededVecs = invIndexes.filter(_._2.indices.nonEmpty).keys.collect().toSet

        val par = partitioner.prepareTasksForParallelization(partitionedVectors, numBuckets, neededVecs, needsSplitting)
        //        val parCount = par.countByKey().toList.sortBy(_._2)
        //        parCount.foreach { case (idx, count) => log.info(s"partition $idx had $count vectors to calculate") }
        val partitionedTasks: RDD[(Int, (Iterable[(Int, VectorWithNorms)], Iterable[(InvertedIndex)]))] = par.cogroup(invIndexes, 30)
        partitionedTasks
    }

    def calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedTasks: RDD[(Int, (Iterable[(Int, VectorWithNorms)], Iterable[InvertedIndex]))], threshold: Double, numBuckets: Int): RDD[(Long, Long, Double)] = {
        println(s"num partitions: ${partitionedTasks.partitions.length}")
        val similarities: RDD[List[Similarity]] = partitionedTasks.mapPartitions {
            iter =>
                iter.map {
                    case (idx, (vectors, i)) =>
                        // there should only be one inverted index
                        require(i.nonEmpty, s"there was no invertedIndex for this bucket with key $idx")
                        val inv = i.head
                        val (bucket, invertedIndex) = (inv.bucket, inv.indices)
                        println(s"calculating similarity for partition: $bucket")
                        val indexMap = InvertedIndex.extractIndexMap(inv)
                        //TODO now that I found the source of the memory problem, is this still required?
                        val answer = new BoundedPriorityQueue[Similarity](1000)(Similarity.orderingBySimilarity)
                        val score = new Array[Double](indexMap.size)
                        vectors.foreach {
                            case (buck, v_j) =>
                                var r_j = v_j.l1
                                val vec = v_j.vector
                                val mutualVectorFeatures = vec.indices.zipWithIndex.filter(b => invertedIndex.contains(b._1))
                                mutualVectorFeatures.foreach {
                                    case (featureIndex, ind_j) =>
                                        val weight_j = vec.values(ind_j)
                                        invertedIndex(featureIndex).foreach {
                                            case (featurePair) => {
                                                val (ind_i, weight_i) = (featurePair.id, featurePair.weight)
                                                val l = indexMap(ind_i)
                                                //TODO _1 is sloppy
                                                if (!((score(l) + inv.metrics(ind_i)._1 * r_j) < threshold))
                                                    score(l) += weight_i * weight_j
                                            }
                                                r_j -= weight_j
                                        }
                                }
                                //record results
                                indexMap.keys.foreach {
                                    ind_i =>
                                        val l = indexMap(ind_i)
                                        val ind_j = v_j.index
                                        val norm_i =  inv.metrics(ind_i)._2
                                        val denom = v_j.normalizer * norm_i
                                        if (score(l) > threshold && ind_i != ind_j) {
                                            val c = Similarity(ind_i, ind_j.toLong, score(l)/denom)
                                            answer += c
                                        }
                                }
                                for (l <- score.indices) {
                                    score(l) = 0
                                }

                        }
                        answer.toList
                }
        }

        similarities.flatMap(x => x).map(s => (s.i, s.j, s.similarity))
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
