package edu.ucsb.apss.InvertedIndex

import edu.ucsb.apss.VectorWithNorms
import org.apache.spark.mllib.linalg.SparseVector



/**
  * Created by dimberman on 1/14/16.
  */
case class InvertedIndex(indices: Map[Int, List[FeaturePair]])

object InvertedIndex {
    private def merge(a: InvertedIndex, b: InvertedIndex): InvertedIndex = {
        InvertedIndex(mergeMap(a.indices,b.indices)((v1,v2) => v1++v2))
    }

//    def add(a:InvertedIndex, v:(Int, List[FeaturePair])) ={
//        if(a.indices.contains(v._1)) InvertedIndex(addMapWithListValue(a.indices, v)((b, v1) => b ++ v1 ))
//    }


    def addInvertedIndexes:(InvertedIndex, Array[(Int,List[FeaturePair])]) => InvertedIndex = (a, b) => InvertedIndex.merge(a, new InvertedIndex(b.toMap))





    def mergeInvertedIndexes:(InvertedIndex, InvertedIndex) => InvertedIndex = (a, b) => InvertedIndex.merge(a, b)



    def createFeaturePairs(vector:VectorWithNorms) = {
//        val vecto = a
        vector.vector.indices.map(i => (i, List(FeaturePair(vector.index, vector.vector(i)))))
    }

    def apply(a:VectorWithNorms):InvertedIndex = {
        new InvertedIndex(createFeaturePairs(a).toMap)
    }

    def apply(a:List[(Int,List[FeaturePair])])= new InvertedIndex(a.toMap)


    def apply() = {
        new InvertedIndex(Map())
    }


//    private def addMapWithListValue[A, B](a: Map[A, B], kv: (A,Array[B]))(f: (B, B) => B): Map[A, B] =
//            a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else (kv._1, kv._2))



    private def mergeMap[A, B](a: Map[A, B], b: Map[A, B])(f: (B, B) => B): Map[A, B] =
        (a /: (for (kv <- b) yield kv)) {
            (c, kv) =>
                c + (if (a.contains(kv._1)) kv._1 -> f(c(kv._1), kv._2) else kv)
        }
}


case class FeaturePair(id: Long, weight: Double)