package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.InvertedIndex.InvertedIndex._
import edu.ucsb.apss.InvertedIndex.InvertedIndex
import edu.ucsb.apss.VectorWithNorms

import scala.collection.mutable

//import edu.ucsb.apss.metrics.PartitionMap
import edu.ucsb.apss.partitioning.HoldensPartitioner
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import scala.collection.mutable.{HashMap => MMap}


import scala.collection.mutable.{ListBuffer, ArrayBuffer}


import scala.collection.mutable.{ListBuffer, ArrayBuffer}


/**
  * Created by dimberman on 1/3/16.
  */

class HoldensPSSDriver {
    val partitioner = HoldensPartitioner
    //    @transient lazy val log = Logger.getLogger(getClass.getName)
    val log = Logger.getLogger(getClass.getName)
    var bucketSize: Long = 0
    var tot = 0L
    var sPar = 0L
    var dPar = 0L
    var numVectors = 0L


    type BucketizedVector = ((Int, Int), VectorWithNorms)

    def bucketizeVectors(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double): RDD[((Int, Int), VectorWithNorms)] = {
        //        sContext = sc
        val count = vectors.count
        numVectors = count
        val normalizedVectors = vectors.map {
            a =>
                val normalizer = HoldensPartitioner.normalizer(a)
                for (i <- a.values.indices) a.values(i) = a.values(i) / normalizer
                a
        }
        val l1partitionedVectors = partitioner.partitionByL1Sort(normalizedVectors, numBuckets, count)
        bucketSize = l1partitionedVectors.countByKey().head._2
        val bucketLeaders = partitioner.determineBucketLeaders(l1partitionedVectors).collect().sortBy(_._1)
        partitioner.tieVectorsToHighestBuckets(l1partitionedVectors, bucketLeaders, threshold, sc)
    }


    def run(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double) = {
        val bucketizedVectors: RDD[BucketizedVector] = bucketizeVectors(sc, vectors, numBuckets, threshold).repartition(30)


        staticPartitioningBreakdown(bucketizedVectors, threshold, numBuckets)


        val numParts = (numBuckets * (numBuckets + 1)) / 2

        //        val needsSplitting = bucketizedVectors.countByKey().filter(_._2 > 2500).map { case ((a, b), c) => ((a.toInt, b.toInt), c) }.toMap

        val invertedIndexes = generateInvertedIndexes(bucketizedVectors, Map(), numParts)
        val partitionedTasks = pairVectorsWithInvertedIndex(bucketizedVectors, invertedIndexes, numBuckets, Map())

        val a: RDD[(Long, Long, Double)] = calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedTasks, threshold, numBuckets)



        a

    }


    def staticPartitioningBreakdown(bucketizedVectors: RDD[BucketizedVector], threshold: Double, numBuckets: Int): Unit = {
        //        var skipped: Long = 0

        val bv = bucketizedVectors.collect()
        val breakdown = bucketizedVectors.countByKey()
        //        breakdown.foreach { case (k, v) => skipped += k._2 * bucketSize * v }

        log.info("breakdown: *******************************************************")
        log.info("breakdown: *******************************************************")
        log.info(s"breakdown: ******** computing PSS with a threshold of $threshold ********")

        val numVecs = breakdown.values.sum
        log.info("breakdown: number of vectors: " + numVecs)
        log.info("breakdown: total number of pairs: " + numVecs * numVecs)
        val skippedPairs = breakdown.toList.map {
            case ((b, t), v) =>
                val modded = if (b == t) 0 else t + 1
                ((b, t), v * modded * bucketSize)
        }.map(a => a._2).sum
        println(numBuckets)
        val keptPairs = breakdown.toList.map {
            case ((b, t), v) =>
                val modded = if (b == t) 0 else t + 1
                ((b, t), bucketSize * (numBuckets - modded) * v)
        }.map(a => a._2).sum
        log.info("breakdown: *******************************************************")

        log.info(s"breakdown: static partitioning:")

        log.info(s"breakdown: skipped pairs: $skippedPairs")
        log.info(s"breakdown: kept pairs: $keptPairs")
        log.info(s"breakdown: ${(numVecs * numVecs) - keptPairs - skippedPairs} unnacounted for")
        sPar = skippedPairs

        val total = skippedPairs + keptPairs

        tot = total
        log.info("breakdown: *******************************************************")
        log.info("breakdown: bucket breakdown:")

        breakdown.toList.map { case ((b, t), v) =>
            val modded = if (b == t) 0 else t + 1
            ((b, t), (v * modded * bucketSize, v))
        }.sortBy(_._1).foreach {
            case (k, (v, n)) =>
                log.info(s"breakdown: $k: $n vectors. $v skipped")
        }
        log.info("breakdown: ")
        log.info("breakdown: ")

    }


    def pullKey(a: (Int, Int)) = (a._1 * (a._1 + 1)) / 2 + a._2


    def pairVectorsWithInvertedIndex(partitionedVectors: RDD[((Int, Int), VectorWithNorms)], invIndexes: RDD[((Int, Int), (InvertedIndex))], numBuckets: Int, needsSplitting: Map[(Int, Int), Long]): RDD[(Int, (Iterable[VectorWithNorms], Iterable[InvertedIndex]))] = {
        val neededVecs = invIndexes.filter(_._2.indices.nonEmpty).keys.sortBy(a => a).collect().toList

        //        log.info("needed vecs:")
        //        log.info(neededVecs.foldRight("")(_+","+_))
        //        log.info(neededVecs.map(input => input._1*(input._1 + 1)/2 + 1 + input._2).foldRight("")(_+","+_))

        val par = partitioner.prepareTasksForParallelization(partitionedVectors, numBuckets, neededVecs, needsSplitting).persist()
        val changed = invIndexes.map { case (a, b) =>
            def partitionHash(input: (Int, Int)) = {
                input._1 * (input._1 + 1) / 2 + 1 + input._2
            }
            (partitionHash(a), b)
        }
        val partitionedTasks: RDD[(Int, (Iterable[VectorWithNorms], Iterable[(InvertedIndex)]))] = par.cogroup(changed, 30).repartition(24)
        partitionedTasks
    }


    def calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedTasks: RDD[(Int, (Iterable[VectorWithNorms], Iterable[InvertedIndex]))], threshold: Double, numBuckets: Int): RDD[(Long, Long, Double)] = {
        //        log.info(s"num partitions: ${partitionedTasks.partitions.length}")
        val skipped: Accumulator[Long] = partitionedTasks.context.accumulator[Long](0)
        val reduced: Accumulator[Long] = partitionedTasks.context.accumulator[Long](0)
        val all: Accumulator[Long] = partitionedTasks.context.accumulator[Long](0)
        val indx: Accumulator[Long] = partitionedTasks.context.accumulator[Long](0)

        partitionedTasks.count()



        val similarities: RDD[Similarity] = partitionedTasks.flatMap {
            case (id, (vectors, i)) =>
                //                // there should only be one inverted index
                require(i.nonEmpty, s"there was no invertedIndex for this bucket with key ($id)")
                //                val answer = ListBuffer.empty[Similarity]
                val answer = new BoundedPriorityQueue[Similarity](1000)

                i.foreach {
                    case (inv) =>
                        val (bucket, invertedIndex) = (inv.bucket, inv.indices)

                        //                log.info(s"calculating similarity for partition: $bucket")
                        val indexMap = InvertedIndex.extractIndexMap(inv)
                        val score = new Array[Double](indexMap.size)
                        vectors.foreach {
                            case v_j =>
                                var r_j = v_j.l1
                                val vec = v_j.vector
//                                val mutualVectorFeatures = vec.indices.zipWithIndex.filter(b => invertedIndex.contains(b._1))
                                var j = 0
                                vec.indices.foreach {
                                    case (featureIndex) =>
                                        if (invertedIndex.contains(featureIndex)) {
                                            val weight_j = vec.values(j)
                                            invertedIndex(featureIndex).foreach {
                                                case (featurePair) => {
                                                    val (ind_i, weight_i) = (featurePair.id, featurePair.weight)
                                                    val l = indexMap(ind_i)
                                                    //                                                if (!((score(l) + inv.maxMap(ind_i) * r_j) < threshold))
                                                    score(l) += weight_i * weight_j
                                                }
                                                //                                                r_j -= weight_j
                                            }
                                        }
                                        j += 1
                                }


//                                mutualVectorFeatures.foreach {
//                                    case (featureIndex, ind_j) =>
//                                        val weight_j = vec.values(ind_j)
//                                        invertedIndex(featureIndex).foreach {
//                                            case (featurePair) => {
//                                                val (ind_i, weight_i) = (featurePair.id, featurePair.weight)
//                                                val l = indexMap(ind_i)
//                                                //                                                if (!((score(l) + inv.maxMap(ind_i) * r_j) < threshold))
//                                                score(l) += weight_i * weight_j
//                                            }
//                                            //                                                r_j -= weight_j
//                                        }
//                                }
                                indexMap.keys.foreach {
                                    ind_i =>
                                        val l = indexMap(ind_i)
                                        val ind_j = v_j.index
                                        if (score(l) > threshold && ind_i != ind_j) {
                                            val c = Similarity(ind_i, ind_j.toLong, score(l))
                                            answer += c
                                            all += 1
                                            reduced += 1
                                        }
                                        else {
                                            //                                            log.info(s"skipped vector pair ($ind_i, $ind_j) with score ${score(l)}")
                                            skipped += 1
                                            all += 1
                                        }
                                }
                                for (l <- score.indices) {
                                    score(l) = 0
                                }
                        }


                }
                //                  List()
                answer
        }.persist()
        similarities.count()
        log.info("breakdown: *******************************************************")
        log.info("breakdown: dynamic partitioning:")

        log.info(s"breakdown: ${all.value} pairs considered after duplicate pair removal")

        log.info("breakdown: " + skipped.value + " vector pairs skipped due to dynamic partitioning")
        dPar = all.value
        log.info("breakdown: " + reduced.value + " vector pairs returned after dynamic partitioning")
        //        log.info("breakdown: index vecs " + indx.value)
        log.info("breakdown: " + (all.value - skipped.value - reduced.value) + " values unaccounted for")
        similarities.map(s => (s.i, s.j, s.similarity))
    }
}
