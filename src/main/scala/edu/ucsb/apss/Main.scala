package edu.ucsb.apss

import edu.ucsb.apss.PSS.PSSDriver
import edu.ucsb.apss.preprocessing.TextToVectorConverter
import org.apache.log4j.Logger

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by dimberman on 1/23/16.
  */


case class Sim(i: Long, j: Long, sim: Double) extends Ordered[Sim] {
    override def compare(that: Sim): Int = this.sim compare that.sim

    override def toString() = s"($i,$j): $sim"
}


case class PSSConfig(
                      input: String = "",
                      thresholds: Seq[Double] = Seq(0.9),
                      numLayers: Int = 21,
                      balanceStage1: Boolean = true,
                      balanceStage2: Boolean = true,
                      output: String = "/tmp/",
                      histTitle: String = "histogram",
                      debug: Boolean = false

                    )

object Main {

    val log = Logger.getLogger(this.getClass)

    def main(args: Array[String]) {

        val opts = new scopt.OptionParser[PSSConfig]("PSS") {

            opt[String]('i', "input")
              .required()
              .action { (x, c) =>
                  c.copy(input = x)
              } text "input is the input file"

            opt[Seq[Double]]('t', "threshold")
              .optional()
              .action { (x, c) =>
                  c.copy(thresholds = x)
              } text "threshold is the threshold for PSS, defaults to 0.9"

            opt[Int]('n', "numLayers")
              .optional()
              .action { (x, c) =>
                  c.copy(numLayers = x)
              } text "number of layers in PSS, defaults to 21"
            opt[String]('o', "output")
              .optional()
              .action { (x, c) =>
                  c.copy(output = x)
              } text "output directory for APSS, defaults to /user/output"
            opt[String]('h', "histogram-title")
              .optional()
              .action { (x, c) =>
                  c.copy(histTitle = x)
              } text "title for histogram, defaults to \"histogram\""
            opt[Boolean]('d', "debug")
              .optional()
              .action { (x, c) =>
                  c.copy(debug = x)
              } text "toggle debug logging. defaults to false"
        }


        opts.parse(args, PSSConfig()) foreach {
            case conf =>
                run(conf)
        }


    }


    def run(config: PSSConfig) = {
        val conf = new SparkConf().setAppName("apss test").set("spark.dynamicAllocation.initialExecutors", "5").set("spark.yarn.executor.memoryOverhead", "600")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)
        val par = sc.textFile(config.input)
        println(s"taking in from ${config.input}")
        println(s"default par: ${sc.defaultParallelism}")

        val executionValues = config.thresholds
        val buckets = config.numLayers
        val vecs = par.map((new TextToVectorConverter).convertTweetToVector(_))
        val theoreticalStaticPartitioningValues = ArrayBuffer[Long]()
        val unbalancedStdDevs = ArrayBuffer[Double]()
        val balancedStdDevs = ArrayBuffer[Double]()

        val actualStaticPartitioningValues = ArrayBuffer[Long]()
        val dynamicPartitioningValues = ArrayBuffer[Long]()
        val timings = ArrayBuffer[Long]()


        val driver = new PSSDriver((config.balanceStage1, config.balanceStage2))



        for (i <- executionValues) {
            val threshold = i
            val t1 = System.currentTimeMillis()
            driver.calculateCosineSimilarity(sc, vecs, buckets, threshold, debug = config.debug, outputDirectory = config.output + s"${sc.applicationId}/output").count()
            val current = System.currentTimeMillis() - t1
            log.info(s"breakdown: apss with threshold $threshold using $buckets buckets took ${current / 1000} seconds")


            unbalancedStdDevs.append(driver.unbalStdDev)
            balancedStdDevs.append(driver.balStdDev)
            theoreticalStaticPartitioningValues.append(driver.theoreticalStaticPairReduction)
            actualStaticPartitioningValues.append(driver.actualStaticPairReduction)
            dynamicPartitioningValues.append(driver.dParReduction)
            timings.append(current / 1000)
        }





        val numPairs = driver.numVectors * driver.numVectors / 2
        log.info("breakdown:")
        log.info("breakdown:")
        log.info(s"breakdown: ************${config.histTitle}******************")
        //        log.info("breakdown:," + buckets.foldRight("")((a,b) => a + "," + b))
        log.info("breakdown:threshold," + executionValues.mkString(","))
        log.info("breakdown: theoretical pairs removed," + theoreticalStaticPartitioningValues.mkString(","))
        log.info("breakdown: actual pairs removed," + theoreticalStaticPartitioningValues.mkString(","))
        log.info("breakdown: theoretical % reduction," + theoreticalStaticPartitioningValues.map(a => a.toDouble / numPairs * 100).map(truncateAt(_, 2)).map(_ + "%").mkString(","))
        log.info("breakdown:actual % reduction," + actualStaticPartitioningValues.map(a => a.toDouble / numPairs * 100).map(truncateAt(_, 2)).map(_ + "%").mkString(","))
        log.info("breakdown:dynamic pairs filtered," + dynamicPartitioningValues.foldRight("")((a, b) => a + "," + b))
        if(config.debug) log.info("breakdown: unbalanced std dev," + unbalancedStdDevs.mkString(","))
        if(config.debug) log.info("breakdown: balanced std dev," + balancedStdDevs.mkString(","))
        log.info("breakdown:timing," + timings.mkString(","))
    }

    def truncateAt(n: Double, p: Int): Double = {
        val s = math pow(10, p);
        (math floor n * s) / s
    }

}
