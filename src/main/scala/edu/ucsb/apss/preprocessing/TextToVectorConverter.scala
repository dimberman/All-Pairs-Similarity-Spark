package edu.ucsb.apss.preprocessing

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vectors, Vector, SparseVector}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by dimberman on 12/7/15.
  */
class TextToVectorConverter extends Serializable{

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

  def convertTweetToVector(s:String, topToRemove:Int = 0, removeSWords:Boolean = false, maxWeight:Int = Int.MaxValue):SparseVector = {
    val table =  generateTable(maxWeight)
    val filtered = if(removeSWords) removeStopWords(s.split(" ").toSeq) else s.split(" ")
    table.transform(s.split(" ").toSeq).toSparse
  }




  def removeStopWords(s:Seq[String]):Seq[String] = {
      s.filterNot(stopWords.contains)
  }

  def generateTable(maxWeight:Int) = {
    new HashingTF(10000){
      override def transform(document: Iterable[_]): Vector = {
        val termFrequencies = mutable.HashMap.empty[Int, Double]
        document.foreach { term =>
          val i = indexOf(term)
          val t= termFrequencies.getOrElse(i, 0.0)
          if (t < maxWeight)
            termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
        }
        Vectors.sparse(numFeatures, termFrequencies.toSeq)
      }
    }
  }

}
