package edu.ucsb.apss.holdensDissimilarity

import edu.ucsb.apss.InvertedIndex.InvertedIndex._
import edu.ucsb.apss.InvertedIndex.InvertedIndex
import edu.ucsb.apss.VectorWithNorms

//import edu.ucsb.apss.metrics.PartitionMap
import edu.ucsb.apss.partitioning.HoldensPartitioner
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import scala.collection.mutable.{HashMap => MMap}


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
        val partitionedTasks = pairVectorsWithInvertedIndex(bucketizedVectors, invertedIndexes, numBuckets, Map()).persist(StorageLevel.MEMORY_AND_DISK_SER)

        val a: RDD[(Long, Long, Double)] = calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedTasks, threshold, numBuckets)



        a

    }


    def staticPartitioningBreakdown(bucketizedVectors: RDD[BucketizedVector], threshold: Double, numBuckets: Int): Unit = {
        var skipped: Long = 0

        val bv = bucketizedVectors.collect()
        val breakdown = bucketizedVectors.countByKey()
        breakdown.foreach { case (k, v) => skipped += k._2 * bucketSize * v }

        sPar = skipped
        println(s"$skipped vector comparisons skipped due to static partitioning with a threshold of $threshold")
        println()
        println("***************************************")
        println()
        println("breakdown:")
        val numVecs = breakdown.values.sum
        println("number of vectors: " + numVecs)
        println("total number of pairs: " + numVecs * numVecs)
        val skippedPairs = breakdown.toList.map { case ((b, t), v) => ((b, t), v * t * bucketSize) }.map(a => a._2).sum
        val keptPairs = breakdown.toList.map { case ((b, t), v) => ((b, t), bucketSize * (numBuckets - t) * v) }.map(a => a._2).sum
        println(s"skipped pairs: $skippedPairs")
        println(s"computed pairs: $keptPairs")
        println(s"${(numVecs * numVecs) - keptPairs - skipped} unnacounted for")

        val total = skippedPairs + keptPairs

        println("total considered: " + total)
        println("total after static partitioning: " + (total - skipped))

        tot = total

        breakdown.toList.map { case ((b, t), v) => ((b, t), (v * t * bucketSize, v)) }.sortBy(_._1).foreach { case (k, (v,n)) => println(s"$k: $n vectors. $v skipped") }
        println()
        println("***************************************")
        println()

    }


    def pullKey(a: (Int, Int)) = (a._1 * (a._1 + 1)) / 2 + a._2


    def pairVectorsWithInvertedIndex(partitionedVectors: RDD[((Int, Int), VectorWithNorms)], invIndexes: RDD[((Int,Int), (InvertedIndex))], numBuckets: Int, needsSplitting: Map[(Int, Int), Long]): RDD[((Int,Int), (Iterable[VectorWithNorms], Iterable[InvertedIndex]))] = {
        val neededVecs = invIndexes.filter(_._2.indices.nonEmpty).keys.sortBy(a => a).collect().toList

        val par = partitioner.prepareTasksForParallelization(partitionedVectors, numBuckets, neededVecs, needsSplitting).persist()
        val partitionedTasks: RDD[((Int,Int), (Iterable[VectorWithNorms], Iterable[(InvertedIndex)]))] = par.cogroup(invIndexes, 30)
        partitionedTasks
    }


    def calculateCosineSimilarityUsingCogroupAndFlatmap(partitionedTasks: RDD[((Int,Int), (Iterable[VectorWithNorms], Iterable[InvertedIndex]))], threshold: Double, numBuckets: Int): RDD[(Long, Long, Double)] = {
        println(s"num partitions: ${partitionedTasks.partitions.length}")
        val skipped: Accumulator[Int] = partitionedTasks.context.accumulator[Int](0)
        val reduced: Accumulator[Int] = partitionedTasks.context.accumulator[Int](0)
        val all: Accumulator[Int] = partitionedTasks.context.accumulator[Int](0)
        val indx: Accumulator[Int] = partitionedTasks.context.accumulator[Int](0)
        val similarities: RDD[Similarity] = partitionedTasks.flatMap {
            case ((buck,tl), (vectors, i)) =>
                // there should only be one inverted index
                require(i.nonEmpty, s"there was no invertedIndex for this bucket with key ($buck, $tl)")
                val answer = ListBuffer.empty[Similarity]
                i.foreach {
                    case (inv) =>
                        val (bucket, invertedIndex) = (inv.bucket, inv.indices)

                        //                println(s"calculating similarity for partition: $bucket")
                        val indexMap = InvertedIndex.extractIndexMap(inv)
                        val score = new Array[Double](indexMap.size)
                        vectors.foreach {
                            case  v_j =>
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
//                                                if (!((score(l) + inv.maxMap(ind_i) * r_j) < threshold))
                                                    score(l) += weight_i * weight_j
                                            }
//                                                r_j -= weight_j
                                        }
                                }
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
//                                            println(s"skipped vector pair ($ind_i, $ind_j) with score ${score(l)}")
                                            skipped += 1
                                            all += 1
                                        }
                                }
                                for (l <- score.indices) {
                                    score(l) = 0
                                }
                        }
                }

                answer
        } .persist()
        similarities.count()
        println(s"${all.value} pairs considered after duplicate pair removal")
        println(skipped.value + " vector pairs skipped due to dynamic partitioning")
        dPar = all.value
        println(reduced.value + " vector pairs returned after dynamic partitioning")
        println("index vecs " + indx.value)
        println((tot/2 - sPar - (dPar-numVectors)) + " values unaccounted for")
        similarities.map(s => (s.i, s.j, s.similarity))
    }

}
