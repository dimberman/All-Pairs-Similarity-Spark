package edu.ucsb.apss.preprocessing

import edu.ucsb.apss.util.PartitionUtil._
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vectors, Vector, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import scala.collection.mutable.{Map => MMap}

import scala.collection.mutable

/**
  * Created by dimberman on 12/7/15.
  */
class TextToVectorConverter extends Serializable {

    val numFeatures = 10000
    val stopWords = Set("the", "and",
        "a",
        "about",
        "above",
        "after",
        "again",
        "against",
        "all",
        "am",
        "an",
        "and",
        "any",
        "are",
        "aren't",
        "as",
        "at",
        "be",
        "because",
        "been",
        "before",
        "being",
        "below",
        "between",
        "both",
        "but",
        "by",
        "can't",
        "cannot",
        "could",
        "couldn't",
        "did",
        "didn't",
        "do",
        "does",
        "doesn't",
        "doing",
        "don't",
        "down",
        "during",
        "each",
        "few",
        "for",
        "from",
        "further",
        "had",
        "hadn't",
        "has",
        "hasn't",
        "have",
        "haven't",
        "having",
        "he",
        "he'd",
        "he'll",
        "he's",
        "her",
        "here",
        "here's",
        "hers",
        "herself",
        "him",
        "himself",
        "his",
        "how",
        "how's",
        "i",
        "i'd",
        "i'll",
        "i'm",
        "i've",
        "if",
        "in",
        "into",
        "is",
        "isn't",
        "it",
        "it's",
        "its",
        "itself",
        "let's",
        "me",
        "more",
        "most",
        "mustn't",
        "my",
        "myself",
        "no",
        "nor",
        "not",
        "of",
        "off",
        "on",
        "once",
        "only",
        "or",
        "other",
        "ought",
        "our",
        "ours",
        "ourselves",
        "out",
        "over",
        "own",
        "same",
        "shan't",
        "she",
        "she'd",
        "she'll",
        "she's",
        "should",
        "shouldn't",
        "so",
        "some",
        "such",
        "than",
        "that",
        "that's",
        "the",
        "their",
        "theirs",
        "them",
        "themselves",
        "then",
        "there",
        "there's",
        "these",
        "they",
        "they'd",
        "they'll",
        "they're",
        "they've",
        "this",
        "those",
        "through",
        "to",
        "too",
        "under",
        "until",
        "up",
        "very",
        "was",
        "wasn't",
        "we",
        "we'd",
        "we'll",
        "we're",
        "we've",
        "were",
        "weren't",
        "what",
        "what's",
        "when",
        "when's",
        "where",
        "where's",
        "which",
        "while",
        "who",
        "who's",
        "whom",
        "why",
        "why's",
        "with",
        "won't",
        "would",
        "wouldn't",
        "you",
        "you'd",
        "you'll",
        "you're",
        "you've",
        "your",
        "yours",
        "yourself",
        "yourselves")


    def gatherCorpusWeightFilter(r: RDD[String], maxDfWeight: Int = Int.MaxValue, filterLonely:Boolean = false) = {
        val table = new HashingTF(10000)

        val map = r.map(s => table.transform(s.split(" ")).toSparse)
          .map(s => MMap() ++ s.indices.zip(s.values).toMap).reduce {
            case (a, b) =>
                b.keys.foreach {
                    k =>
                        if (a.contains(k)) a(k) += b(k)
                        else a(k) = b(k)
                }
                a
        }


        val x = r.map(s => table.transform(s.split(" ")).toSparse)
          .flatMap{
              case(words) =>
                  words.indices.map((_,1))
          }.collect()

        val topFeatures = r.map(s => table.transform(s.split(" ")).toSparse)
          .flatMap{
              case(words) =>
                  words.indices.map((_,1))
        }.reduceByKey(_+_).sortBy(-_._2).keys.take(10000/20).toSet



        map.filter{case(i,weight) => weight> maxDfWeight}.keySet.toSet ++ topFeatures


    }

    def convertTextToVector(s: String, topToRemove: Int = 0, removeSWords: Boolean = false, maxWeight: Int = Int.MaxValue, dfFilterSet: Set[Int] = Set()): SparseVector = {
        val table = new HashingTF(10000)
        val filtered = if (removeSWords) removeStopWords(s.split(" ").toSeq) else s.split(" ").toSeq
        val vec = table.transform(filtered)
        val ans = vec.toSparse

        val x = dfFilterSet
        val filterVals = ans.indices.zip(ans.values).filter { case (a, b) => b > maxWeight }.map(_._1).toSet  ++ dfFilterSet
        var i = 0
        //        val max5 =  ans.indices.zip(ans.values).sortBy(-_._2).take(ans.values.size/20).map(_._1).toSet
        val max5 = Set[Int]()

        //        val dfInd = ans.indices.filter( dfFilterSet.contains)


        val answer = ans.indices.zip(ans.values)
          //          .filterNot{ case(a,b) => dfFilterSet.contains(a) || max5.contains(a)}
          .unzip
        val (filterIndices, filteredValues) = answer

        normalizeVector(ans, filterVals)

    }

    def removeStopWords(s: Seq[String]): Seq[String] = {
        s.filterNot(stopWords.contains)
    }


    def nonNegativeMod(x: Int, mod: Int): Int = {
        val rawMod = x % mod
        rawMod + (if (rawMod < 0) mod else 0)
    }


    def generateTable(maxWeight: Int) = {
        new HashingTF(10000) {

        }
    }

}
