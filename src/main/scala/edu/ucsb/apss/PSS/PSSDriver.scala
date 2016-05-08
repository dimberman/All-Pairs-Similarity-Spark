package edu.ucsb.apss.PSS

import edu.ucsb.apss.Accumulators._
import edu.ucsb.apss.InvertedIndex.InvertedIndex._
import edu.ucsb.apss.InvertedIndex.{FeaturePair, SimpleInvertedIndex, InvertedIndex}
import edu.ucsb.apss.util.{VectorWithNorms, BoundedPriorityQueue, FileSystemManager}


import edu.ucsb.apss.partitioning.{LoadBalancer, StaticPartitioner}
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


class PSSDriver(loadBalance: (Boolean, Boolean) = (true, true)) {

    import edu.ucsb.apss.util.PartitionUtil._
    import edu.ucsb.apss.PSS.SimilarityCalculator._

    import StaticPartitioner._

    var debugPSS = true
    val partitioner = StaticPartitioner
    //    @transient lazy val log = Logger.getLogger(getClass.getName)
    val log = Logger.getLogger(getClass.getName)
    var bucketSizes: List[Int] = _
    var tot = 0L
    var theoreticalStaticPairReduction = 0L

    var actualStaticPairReduction = 0L

    var dParReduction = 0L
    var bucketizedVectorSizeMap: Map[(Int, Int), Int] = _
    var appId: String = _
    var sPar = 0L
    var dPar = 0L
    var numVectors = 0L
    var numComparisons = 0L
    val manager = new FileSystemManager

    type BucketizedVector = ((Int, Int), VectorWithNorms)


    def run(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double, calculationSize: Int = 100, debug: Boolean = true) = {
        debugPSS = debug
        val l1partitionedVectors = bucketizeVectors(sc, vectors, numBuckets, threshold)
        val staticPartitionedVectors = staticPartition(l1partitionedVectors, threshold, sc)

        if (debugPSS) logStaticPartitioning(staticPartitionedVectors, threshold, numBuckets)

        val invertedIndexes = generateInvertedIndexes(staticPartitionedVectors, 100)

        manager.writePartitionsToFile(staticPartitionedVectors)

        val balanceMapping = balancePSS(invertedIndexes, numBuckets)
        log.info("breakdown: balancing finished. beginning PSS")

        calculateCosineSimilarityByPullingFromFile(invertedIndexes, threshold, numBuckets, balanceMapping)
    }


    def bucketizeVectors(sc: SparkContext, vectors: RDD[SparseVector], numBuckets: Int, threshold: Double): RDD[(Int, VectorWithNorms)] = {
        val count = vectors.count

        numVectors = count
        numComparisons = (numVectors * (numVectors - 1)) / 2

        val normalizedVectors = vectors.map(normalizeVector)
        val indexedNormalizedVecs = recordIndex(normalizedVectors)
                                        .repartition(numBuckets)

        val l1partitionedVectors = partitionByL1Sort(indexedNormalizedVecs, numBuckets, count).mapValues(extractUsefulInfo)
        bucketSizes = l1partitionedVectors.countByKey().toList.sortBy(_._1).map(_._2.toInt)
        l1partitionedVectors
    }

    def staticPartition(l1partitionedVectors: RDD[(Int, VectorWithNorms)], threshold: Double, sc: SparkContext) = {
        val bucketLeaders = determineBucketLeaders(l1partitionedVectors)
        val sPartitioned = partitioner.tieVectorsToHighestBuckets(l1partitionedVectors, bucketLeaders, threshold, sc)
        bucketizedVectorSizeMap = sPartitioned.countByKey().mapValues(_.toInt).toMap.withDefault(_ => 0)
        bucketizedVectorSizeMap.toList.sortBy(_._1).foreach(println)
        sPartitioned

    }


    def balancePSS(invertedIndexes: RDD[((Int, Int), Iterable[SimpleInvertedIndex])], numBuckets: Int, balance: Boolean = true): Map[(Int, Int), List[(Int, Int)]] = {

        val buckets = invertedIndexes.filter(_._2.nonEmpty).keys.collect()
        val neededVecs = buckets.sortBy(a => a)
        val unbalanced = buckets.map { case (b, t) => ((b, t), LoadBalancer.assignByBucket(b, t, numBuckets, neededVecs)) }.toMap
        val held = unbalanced.toList.flatMap{case(a,b) => b.map(c=> (a,c)).map{case(d,e)=> if(d._1 > e._1 || (d._1 == e._1 && d._2 >= e._2)) (d,e) else (e,d)}}.sorted
        val comparisonList = unbalanced.map {
            case (k, v) =>
                v.map(
                    a =>
                        if (a != k)
                            bucketizedVectorSizeMap(a) * bucketizedVectorSizeMap(k)
                        else
                            bucketizedVectorSizeMap(a) * (bucketizedVectorSizeMap(a) - 1) / 2
                )
        }
        val unbalancedComparisons = comparisonList.map(_.sum).sum

        theoreticalStaticPairReduction = numComparisons - unbalancedComparisons

        log.info("")
        log.info("")
        log.info("static partitioning breakdown:")


        unbalanced.toList.map { case ((b, t), v) =>
            var nvec = 0
            for (i <- 0 to b) nvec += bucketSizes(i)
            if (b == t) {
                ((b, t), (0, nvec))
            }
            else {
                var nvecSkipped = 0
                for (i <- 0 to t){
                    require(bucketSizes(i) >= bucketizedVectorSizeMap((i, i)), s"breakdown: got negative value from bucket $i: size[$i]:${bucketSizes(i)}, partition($i,$i):${bucketizedVectorSizeMap((i, i))}")
                    nvecSkipped += bucketSizes(i)
                }
                ((b, t), (nvecSkipped, nvec))

            }
        }.sortBy(_._1).foreach {
            case (k, (v, n)) =>
                log.info(s"breakdown: $k: $n vectors. $v skipped")
        }



        log.info("breakdown: after duplicate removal skipped pairs: " + (numComparisons -unbalancedComparisons))

        log.info("breakdown: after duplicate removal skip %: " + truncateAt((numComparisons - unbalancedComparisons.toDouble) / numComparisons * 100, 2) + "%")



//        unbalanced

       val ans =  if (balance) LoadBalancer.balance(unbalanced, bucketizedVectorSizeMap, loadBalance, Some(log)) else unbalanced

        log.info("breakdown: banlancing complete")


        ans
    }


    def calculateCosineSimilarityByPullingFromFile(invertedIndexes: RDD[((Int, Int), Iterable[SimpleInvertedIndex])], threshold: Double, numBuckets: Int, balancedMapping: Map[(Int, Int), List[(Int, Int)]], calcSize: Int = 100): RDD[(Long, Long, Double)] = {
        //        log.info(s"num partitions: ${partitionedTasks.partitions.length}")
        val skipped: Accumulator[Long] = invertedIndexes.context.accumulator[Long](0)
        val reduced: Accumulator[Long] = invertedIndexes.context.accumulator[Long](0)
        val all: Accumulator[Long] = invertedIndexes.context.accumulator[Long](0)
        val sc = invertedIndexes.context

        log.info("breakdown: beginning PSS")
        invertedIndexes.count()
        val partMap = LoadBalancer.balanceByPartition(sc.defaultParallelism, balancedMapping, bucketizedVectorSizeMap)
        val balancedPairs = balancedMapping.mapValues(_.size).map(identity)

        //        log.info("breakdown: pre-load balancing")
        //        unbalancedPairs.foreach { case (k, v) => log.info(s"breakdown: $k: $v") }
        //
        //        log.info("breakdown: post-load balancing")
        //        balancedPairs.foreach { case (k, v) => log.info(s"breakdown: $k: $v") }


        //        log.info(s"breakdown: default parallelism: ${sc.defaultParallelism}")

        val balancedInvertedIndexes = invertedIndexes.map {
            case (bucket, inv) =>
                //                if (balancedPairs(bucket) > 11) {
                //                    List((bucket._1, (bucket, inv, (0, 2))), (numBuckets - bucket._1, (bucket, inv, (1, 2))))
                //                }
                //                else {
                (partMap(bucket), (bucket, inv, (0, 1)))
            //                }
        }.repartition(sc.defaultParallelism)

        val BVConf = sc.broadcast(new SerializableWritable(sc.hadoopConfiguration))

        val BVPairs = sc.broadcast(balancedMapping)

        val id = invertedIndexes.context.applicationId
        val buckAccum = invertedIndexes.context.accumulator("", "debug info")(LineAcummulatorParam)
        val driverAccum = invertedIndexes.context.accumulable(ArrayBuffer[DebugVal](), "debug info")(DebugAcummulatorParam)

        log.info(buckAccum.value)

        val similarities: RDD[Similarity] = balancedInvertedIndexes.values.flatMap {
            case (((bucket, tl), invIter, (ind, mod))) =>
                val manager = new FileSystemManager
                var numVecPair = 0
                val start = System.currentTimeMillis()
                val filtered = BVPairs.value((bucket, tl))
                val numBuc = filtered.size
                val answer = new BoundedPriorityQueue[Similarity](1000)
                //                val answer = new ArrayBuffer[Similarity]()

                filtered.foreach {
                    case (key) =>
                        val externalVectors = manager.readVecPartition(key, id, BVConf, org.apache.spark.TaskContext.get()).toList.zipWithIndex.map(_._1)
//                        println(s"comparing ${(bucket,tl)} to $key")
                        invIter.foreach {
                            inv =>
                                val indexMap = InvertedIndex.extractIndexMapFromSimple(inv)
                                val scores = new Array[Double](calcSize)
                                val invertedIndex = inv.indices
                                externalVectors.foreach {
                                    case v_j =>
                                        val VectorWithNorms(_, _, _, vec, ind_j, _) = v_j

                                        calculateScores(vec, invertedIndex, indexMap, scores)

                                        indexMap.foreach {
                                            case (ind_i, l) =>
                                                if(ind_i == ind_j || (bucket, tl) == key && ind_i < ind_j){

                                                }
                                                else if (scores(l) > threshold) {
                                                    val c = Similarity(ind_i, ind_j.toLong, scores(l))
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
                                        clearScoreArray(scores)

                                }


                        }


                }
                val time = (System.currentTimeMillis() - start).toDouble / 1000
                //                driverAccum += s"breakdown: partition ${(inv.bucket,inv.tl)} took $time seconds to calculate $numVecPair pairs from $numBuc buckets"
                driverAccum += DebugVal((bucket, tl), time, numVecPair, numBuc)
                answer.toList
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

        log.info("breakdown: *******************************************************")
        log.info("breakdown: *******************************************************")
        log.info(s"breakdown: ******** computing PSS with a threshold of $threshold ********")

        val numVecs = breakdown.values.sum
        log.info("breakdown: number of vectors: " + numVectors)
        log.info("breakdown: post-bucketization # of vectors: " + numVecs)

        log.info("breakdown: total number of pairs: " + numComparisons)



        for( i <- 0 to numBuckets-1) {
            val bSize = List.range(0,i+1).map( x => bucketizedVectorSizeMap(i,x)).sum
//            println(s"bsize: $bSize")
            require(bucketSizes(i) == bSize, s"the sum of the bucketizedVectorMap values did not equal the bucketSize. Bsize: ${bucketSizes(i)}, parts: ${bucketizedVectorSizeMap.filterKeys(_._1 == i)}")
        }


        val b = bucketizedVectorSizeMap

        val skippedPairs = breakdown.toList.map {
            case ((b, t), v) =>
                require(t >= 0, "negative tiedleader")
                if (b == t) {
                    ((b, t), 0)
                }
                else {
                    var nvec = 0
                    for (i <- 0 to t) {

                        require(bucketSizes(i) >= bucketizedVectorSizeMap((i, i)), s"breakdown: got negative value from bucket $i: size[$i]:${bucketSizes(i)}, partition($i,$i):${bucketizedVectorSizeMap((i, i))}")

                        nvec += v.toInt * bucketSizes(i)
                    }
                    ((b, t), nvec)
                }
        }.map(a => a._2).sum




        log.info("breakdown: *******************************************************")

        log.info(s"breakdown: static partitioning:")


        //        log.info(s"breakdown: kept pairs: $keptPairs")
        //        log.info(s"breakdown: ${(numVecs * numVecs) / 2 - keptPairs - skippedPairs + 10000} unnacounted for")
        sPar = skippedPairs

        //        val total = skippedPairs + keptPairs

        //        tot = total
        //        log.info("breakdown: *******************************************************")
        log.info("breakdown: bucket breakdown:")








        breakdown.toList.map { case ((b, t), v) =>
            var nvec = 0
            for (i <- 0 to b) nvec += bucketSizes(i)
            if (b == t) {
                ((b, t), (0, nvec))
            }
            else {
                var nvecSkipped = 0
                for (i <- 0 to t)
                    nvecSkipped += bucketSizes(i)
                ((b, t), (nvecSkipped, nvec))

            }
        }.sortBy(_._1).foreach {
            case (k, (v, n)) =>
                log.info(s"breakdown: $k: $n vectors. $v skipped")
        }

        val calculatedPairs = breakdown.toList.map {
            case ((b, t), v) =>
                require(t >= 0, "negative tiedleader")
//                if (b == t) {
//                    ((b, t), 0)
//                }
//                else {
                    var nvec = 0
                    for (i <- t+1 to b) {

                        require(bucketSizes(i) >= bucketizedVectorSizeMap((i, i)), s"breakdown: got negative value from bucket $i: size[$i]:${bucketSizes(i)}, partition($i,$i):${bucketizedVectorSizeMap((i, i))}")

                        nvec += v.toInt * bucketSizes(i)
                    }
                    ((b, t), nvec)
//                }
        }.map(a => a._2).sum


        log.info(s"breakdown: theoretical skipped pairs: $skippedPairs")
        log.info(s"breakdown: theoretical calculated pairs: $calculatedPairs")

        log.info("breakdown: theoretical skipped pair %: " + truncateAt(skippedPairs.toDouble / numComparisons * 100, 2) + "%")

    }


    def logDynamicPartitioningOutput(skipped: Accumulator[Long], reduced: Accumulator[Long], postStaticPartitioningPairs: Accumulator[Long], manager: FileSystemManager, sc: SparkContext, BVConf: Broadcast[SerializableWritable[Configuration]], driverAccum: Accumulable[ArrayBuffer[DebugVal], DebugVal], similarities: RDD[Similarity]) = {
        //        log.info(driverAccum.value.sortBy(_.numPairs).map(d => s"breakdown: partition ${d.key} took ${d.time} seconds to calculate ${d.numPairs} pairs from ${d.numBuckets} buckets").mkString("\n"))

        log.info("breakdown: *******************************************************")
        log.info("breakdown: dynamic partitioning:")

        log.info(s"breakdown: ${postStaticPartitioningPairs.value} pairs considered after duplicate pair removal")

        actualStaticPairReduction = numVectors * numVectors / 2 - postStaticPartitioningPairs.value
        dParReduction = skipped.value
        log.info("breakdown: " + skipped.value + " vector pairs skipped due to dynamic partitioning")
        dPar = postStaticPartitioningPairs.value
        log.info("breakdown: " + reduced.value + " vector pairs returned after dynamic partitioning")
        //        log.info("breakdown: index vecs " + indx.value)
        log.info("breakdown: " + (postStaticPartitioningPairs.value - skipped.value - reduced.value) + " values unaccounted for")

        log.info("breakdown: staticPairRemoval," + actualStaticPairReduction)
        log.info("breakdown: skipped pair %: " + truncateAt(actualStaticPairReduction.toDouble / (numVectors * (numVectors -1) / 2) * 100, 2) + "%")

    }
}


//def logScoreToAnswerVector(indexMap: Map[Long,Int], scores:Array[Double], ind_j:Int, ans:): Unit ={
//                    indexMap.foreach {
//                        case (ind_i, l) =>
//                            if (scores(l) > threshold && ind_i != ind_j) {
//                                val c = Similarity(ind_i, ind_j.toLong, scores(l))
//                                answer += c
//                                all += 1
//                                reduced += 1
//                                numVecPair += 1
//
//                            }
//                            else {
//                                //                                            log.info(s"skipped vector pair ($ind_i, $ind_j) with score ${score(l)}")
//                                skipped += 1
//                                all += 1
//                                numVecPair += 1
//
//                            }
//
//                    }
//                }



