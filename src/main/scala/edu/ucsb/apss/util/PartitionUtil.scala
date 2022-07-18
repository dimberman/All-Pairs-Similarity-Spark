package edu.ucsb.apss.util

import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by dimberman on 4/10/16.
  */
object PartitionUtil extends Serializable {
    def l1Norm(v: SparseVector) = {
        v.values.map(math.abs).sum
    }


    def lInfNorm(v: SparseVector) = {
        v.values.map(math.abs).max
    }

    def normalizer(v: SparseVector) = {
        val a = v.values.map(a => a * a).sum
        math.sqrt(a)
    }

    def normalizer(v: Seq[Double]) = {
        val a = v.map(a => a * a).sum
        math.sqrt(a)
    }

    def dotProduct(v1: SparseVector, v2: SparseVector): Double = {
        val v1Map = v1.indices.zip(v1.values).toMap
        var sum = 0.0
        var aVal = 0.0
        var bVal = 0.0
        var num = 0
        for (i <- v2.indices.indices) {
            if (v1Map.contains(v2.indices(i))) {
                sum += v1Map(v2.indices(i)) * v2.values(i)
//                aVal +=  v1Map(v2.indices(i)) * v1Map(v2.indices(i))
//                bVal +=  v2.values(i) * v2.values(i)
            }
        }


        val anorm = math.sqrt(v1.values.map(x => x * x).sum)
        val bnorm = math.sqrt(v2.values.map(x => x * x).sum)

        val answer = sum / (anorm * bnorm)
//        println(s"ideal similarity: $answer")
//        val answer = sum/(math.sqrt(aVal) * math.sqrt(bVal))
        //        val answer =  sum/(math.sqrt(anorm)* math.sqrt(bnorm))


//        println(s"ideal similarity: $answer")
        //        sum /(normalizer(v1)*normalizer(v2))
        answer
    }

    def normalizeVector(vec: SparseVector, filter:Set[Int] = Set()): SparseVector = {

        val (filterInd, filterVals) = vec.indices.zip(vec.values).filterNot{ case(a,b) => filter.contains(a)} .unzip
        val norm = normalizer(filterVals)

        for (i <- filterInd.indices) filterVals(i) = filterVals(i) / norm
        new SparseVector(filterInd.size, filterInd.toArray, filterVals.toArray)

    }




    def extractUsefulInfo(v: VectorWithIndex): VectorWithNorms = {
        val vec = v.vec
        new VectorWithNorms(lInfNorm(vec), l1Norm(vec), normalizer(vec), vec, v.index)
    }

    def truncateAt(n: Double, p: Int): Double = {
        val s = math pow(10, p)
        (math floor n * s) / s
    }


}

case class VectorWithIndex(vec: SparseVector, index: Long)
