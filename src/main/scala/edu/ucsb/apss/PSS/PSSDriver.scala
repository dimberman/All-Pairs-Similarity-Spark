package edu.ucsb.apss.PSS

import edu.ucsb.apss.Accumulators._
import edu.ucsb.apss.InvertedIndex.InvertedIndex._
import edu.ucsb.apss.InvertedIndex.{SimpleInvertedIndex, InvertedIndex}
import edu.ucsb.apss.util.{BoundedPriorityQueue, FileSystemManager}

import edu.ucsb.apss.util.PartitionUtil.VectorWithNorms

import edu.ucsb.apss.partitioning.{LoadBalancer, PartitionHasher, StaticPartitioner}
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import scala.collection.mutable.{HashMap => MMap}


import scala.collection.mutable.ArrayBuffer


/**
  * Created by dimberman on 1/3/16.
  */


class PSSDriver(loadBalance:(Boolean,Boolean) = (true,true)) {

    import edu.ucsb.apss.util.PartitionUtil._
    import StaticPartitioner._

    var debugPSS = true
    val partitioner = StaticPartitioner
    //    @transient lazy val log = Logger.getLogger(getClass.getName)
    val log = Logger.getLogger(getClass.getName)
    var bucketSizes: List[Int] = _
    var tot = 0L
    var sParReduction = 0L
    var dParReduction = 0L
    var bucketizedVectorSizeMap: Map[(Int, Int), Int] = _
    var appId: String = _
    var sPar = 0L
    var dPar = 0L
    var numVectors = 0L


    type BucketizedVector = ((Int, Int), VectorWithNorms)



    def run(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double, calculationSize: Int = 100, debug: Boolean = true) = {
        debugPSS = debug
        val l1partitionedVectors = bucketizeVectors(sc, vectors, numBuckets, threshold)
        val staticPartitionedVectors = staticPartition(l1partitionedVectors, threshold, sc)
        bucketizedVectorSizeMap = staticPartitionedVectors.countByKey().mapValues(_.toInt).toMap.withDefault(_ => -1)
        val manager = new FileSystemManager

        if (debugPSS) logStaticPartitioning(staticPartitionedVectors, threshold, numBuckets)

        val invertedIndexes = generateSplitInvertedIndexes(staticPartitionedVectors, 100)


        manager.writePartitionsToFile(staticPartitionedVectors)

        //        manager.writeInvertedIndexesToFile(invertedIndexes)

        calculateCosineSimilarityByPullingFromFile(invertedIndexes, threshold, numBuckets)
    }


    def bucketizeVectors(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double): RDD[(Int, VectorWithNorms)] = {
        val count = vectors.count
        numVectors = count
        val normalizedVectors = vectors.repartition(numBuckets).map(normalizeVector)
        val indexednVecs = recordIndex(normalizedVectors)
        val l1partitionedVectors = partitionByL1Sort(indexednVecs, numBuckets, count).mapValues(extractUsefulInfo)
        if (debugPSS) bucketSizes = l1partitionedVectors.countByKey().toList.sortBy(_._1).map(_._2.toInt)
        l1partitionedVectors
    }

    def staticPartition(l1partitionedVectors:RDD[(Int,VectorWithNorms)], threshold:Double, sc:SparkContext) = {
        val bucketLeaders = determineBucketLeaders(l1partitionedVectors)
        partitioner.tieVectorsToHighestBuckets(l1partitionedVectors, bucketLeaders, threshold, sc)
    }




    def calculateCosineSimilarityByPullingFromFile(invertedIndexes: RDD[((Int, Int), Iterable[SimpleInvertedIndex])], threshold: Double, numBuckets: Int, calcSize: Int = 100): RDD[(Long, Long, Double)] = {
        //        log.info(s"num partitions: ${partitionedTasks.partitions.length}")
        val skipped: Accumulator[Long] = invertedIndexes.context.accumulator[Long](0)
        val reduced: Accumulator[Long] = invertedIndexes.context.accumulator[Long](0)
        val all: Accumulator[Long] = invertedIndexes.context.accumulator[Long](0)
        val indx: Accumulator[Long] = invertedIndexes.context.accumulator[Long](0)

        invertedIndexes.count()

        val buckets = invertedIndexes.filter(_._2.nonEmpty).keys.collect()
        val neededVecs = buckets.sortBy(a => a)


        val manager = new FileSystemManager
        val unbalanced = buckets.map { case (b, t) => ((b, t), LoadBalancer.assignByBucket(b, t, numBuckets, neededVecs)) }.toMap
        val balancedMapping = LoadBalancer.balance(unbalanced, bucketizedVectorSizeMap, loadBalance)




        val balancedPairs = balancedMapping.mapValues(_.size).map(identity)
//        log.info("breakdown: pre-load balancing")
//        unbalancedPairs.foreach { case (k, v) => log.info(s"breakdown: $k: $v") }
//
//        log.info("breakdown: post-load balancing")
//        balancedPairs.foreach { case (k, v) => log.info(s"breakdown: $k: $v") }


        val sc = invertedIndexes.context

        val partMap = LoadBalancer.balanceByPartition(21, balancedMapping, bucketizedVectorSizeMap)

        val balancedInvertedIndexes = invertedIndexes.map {
            case (bucket, inv) =>
//                if (balancedPairs(bucket) > 11) {
//                    partnum += 2
//                    List((bucket._1, (bucket, inv, (0, 2))), (numBuckets - bucket._1, (bucket, inv, (1, 2))))
//                }
//                else {
                    (partMap(bucket), (bucket, inv, (0, 1)))
//                }
        }.repartition(21)


//        balancedInvertedIndexes.count()
//        val pssInput = balancedInvertedIndexes.repartition(numBuckets)


        val BVConf = sc.broadcast(new SerializableWritable(sc.hadoopConfiguration))

        val BVPairs = sc.broadcast(balancedMapping)

        val id = invertedIndexes.context.applicationId
        val buckAccum = invertedIndexes.context.accumulator("", "debug info")(LineAcummulatorParam)
        val driverAccum = invertedIndexes.context.accumulable(ArrayBuffer[DebugVal](), "debug info")(DebugAcummulatorParam)


        //        val calculatedInvertedIndexes = invertedIndexes.mapPartitionsWithIndex(
        //            (index, iter) => {
        //                val buckets = ArrayBuffer[(Int,Int)]()
        //                iter.foreach {
        //                    case (bucket, inv) =>
        //                        buckets += bucket
        //
        //                }
        //                buckAccum += s"breakdown: ${buckets.mkString(",")} are all in partition $index"
        //                iter
        //            }
        //        )


        log.info(buckAccum.value)

        val similarities: RDD[Similarity] = balancedInvertedIndexes.values.flatMap {
            //        val similarities: RDD[Similarity] = invertedIndexes. flatMap {
            case (((bucket, tl), invIter, (ind, mod))) =>
                //                case (((bucket, tl), invIter)) =>
                val manager = new FileSystemManager
                //                val get = List().toIterator

                var numVecPair = 0
                val start = System.currentTimeMillis()
                //                val get = LoadBalancer.assignByBucket(bucket, tl, numBuckets, neededVecs)
                //                val filtered = get.filter(neededVecs.contains)
                val filtered = BVPairs.value((bucket, tl))
                val numBuc = filtered.size
                //                println(s"breakdown: assignment ${get.mkString(",")}")

                //                val answer = new ArrayBuffer[Similarity]()
                val answer = new BoundedPriorityQueue[Similarity](1000)

                filtered.foreach {
                    case (key) =>
                        val externalVectors = manager.readVecPartition(key, id, BVConf, org.apache.spark.TaskContext.get()).toList.zipWithIndex.map(_._1)
                        invIter.foreach {
                            inv =>
                                val indexMap = InvertedIndex.extractIndexMapFromSimple(inv)
                                val score = new Array[Double](calcSize)
                                val invertedIndex = inv.indices
                                externalVectors.foreach {
                                    case v_j =>
                                        var r_j = v_j.l1
                                        val vec = v_j.vector
                                        //                                        val mutualVectorFexatures = vec.indices.zipWithIndex.filter(b => invertedIndex.contains(b._1))
                                        var j = 0
                                        vec.indices.foreach {
                                            case (featureIndex) =>
                                                if (invertedIndex.contains(featureIndex)) {
                                                    val weight_j = vec.values(j)
                                                    invertedIndex(featureIndex).foreach {
                                                        case (featurePair) => {
                                                            val (ind_i, weight_i) = (featurePair.id, featurePair.weight)
                                                            val l = indexMap(ind_i)
                                                            //                                                    if (!((score(l) + inv.maxMap(ind_i) * r_j) < threshold))
                                                            score(l) += weight_i * weight_j
                                                        }
                                                        //                                                    r_j -= weight_j
                                                    }
                                                }
                                                j += 1
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
                                                    numVecPair += 1

                                                }
                                                else {
                                                    //                                            log.info(s"skipped vector pair ($ind_i, $ind_j) with score ${score(l)}")
                                                    skipped += 1
                                                    all += 1
                                                    numVecPair += 1

                                                }
                                        }
                                        for (l <- score.indices) {
                                            score(l) = 0
                                        }
                                }


                        }


                }
                val time = (System.currentTimeMillis() - start).toDouble / 1000
                //                driverAccum += s"breakdown: partition ${(inv.bucket,inv.tl)} took $time seconds to calculate $numVecPair pairs from $numBuc buckets"
                driverAccum += DebugVal((bucket, tl), time, numVecPair, numBuc)
                answer
        }.persist()
        similarities.count()
        logDynamicPartitioningOutput(skipped, reduced, all, manager, sc, BVConf, driverAccum, similarities)
        manager.cleanup(sc.applicationId, BVConf)
        similarities.map(s => (s.i, s.j, s.similarity))
    }


    def logStaticPartitioning(bucketizedVectors: RDD[BucketizedVector], threshold: Double, numBuckets: Int): Unit = {
        //        var skipped: Long = 0

        val bv = bucketizedVectors.collect()
        val breakdown = bucketizedVectors.countByKey()
        //        breakdown.foreach { case (k, v) => skipped += k._2 * bucketSize * v }
        bucketSizes.foreach(b => println("bucket size: " + b))
        log.info("breakdown: *******************************************************")
        log.info("breakdown: *******************************************************")
        log.info(s"breakdown: ******** computing PSS with a threshold of $threshold ********")

        val numVecs = breakdown.values.sum
        log.info("breakdown: number of vectors: " + numVecs)
        log.info("breakdown: total number of pairs: " + (numVecs * numVecs) / 2)
        val skippedPairs = breakdown.toList.map {
            case ((b, t), v) =>
                if (b == t) {
                    ((b, t), 0)
                }
                else {
                    var nvec = 0
                    for (i <- 0 to t) nvec += v.toInt * bucketSizes(i)
                    ((b, t), nvec)
                }
        }.map(a => a._2).sum
        println(numBuckets)
        val keptPairs = breakdown.toList.map {
            case ((b, t), v) =>
                var nvec = 0
                for (i <- t + 1 to b) nvec += v.toInt * bucketSizes(i)
                ((b, t), nvec)
        }.map(a => a._2).sum
        log.info("breakdown: *******************************************************")

        log.info(s"breakdown: static partitioning:")

        log.info(s"breakdown: skipped pairs: $skippedPairs")
        log.info(s"breakdown: kept pairs: $keptPairs")
        log.info(s"breakdown: ${(numVecs * numVecs) / 2 - keptPairs - skippedPairs + 10000} unnacounted for")
        sPar = skippedPairs

        val total = skippedPairs + keptPairs

        tot = total
//        log.info("breakdown: *******************************************************")
//        log.info("breakdown: bucket breakdown:")
//
//        breakdown.toList.map { case ((b, t), v) =>
//            var nvec = 0
//            for (i <- 0 to b) nvec += bucketSizes(i)
//            if (b == t) {
//                ((b, t), (0, nvec))
//            }
//            else {
//                var nvecSkipped = 0
//                for (i <- 0 to t) nvecSkipped += bucketSizes(i)
//                ((b, t), (nvecSkipped, nvec))
//
//            }
//        }.sortBy(_._1).foreach {
//            case (k, (v, n)) =>
//                log.info(s"breakdown: $k: $n vectors. $v skipped")
//        }
        log.info("breakdown: ")
        log.info("breakdown: ")

    }


    def logDynamicPartitioningOutput(skipped: Accumulator[Long], reduced: Accumulator[Long], all: Accumulator[Long], manager: FileSystemManager, sc: SparkContext, BVConf: Broadcast[SerializableWritable[Configuration]], driverAccum: Accumulable[ArrayBuffer[DebugVal], DebugVal], similarities: RDD[Similarity]) = {
//        log.info(driverAccum.value.sortBy(_.numPairs).map(d => s"breakdown: partition ${d.key} took ${d.time} seconds to calculate ${d.numPairs} pairs from ${d.numBuckets} buckets").mkString("\n"))

        log.info("breakdown: *******************************************************")
        log.info("breakdown: dynamic partitioning:")

        log.info(s"breakdown: ${all.value} pairs considered after duplicate pair removal")

        sParReduction = numVectors * numVectors / 2 - all.value
        dParReduction = skipped.value
        log.info("breakdown: " + skipped.value + " vector pairs skipped due to dynamic partitioning")
        dPar = all.value
        log.info("breakdown: " + reduced.value + " vector pairs returned after dynamic partitioning")
        //        log.info("breakdown: index vecs " + indx.value)
        log.info("breakdown: " + (all.value - skipped.value - reduced.value) + " values unaccounted for")

        log.info("breakdown:staticPairRemoval," + sParReduction)
        log.info("breakdown:static%reduction," + sParReduction.toDouble / (numVectors * numVectors / 2))

    }
}





