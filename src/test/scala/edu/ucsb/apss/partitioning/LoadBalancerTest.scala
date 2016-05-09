package edu.ucsb.apss.partitioning

import com.twitter.chill.{KryoInstantiator, KryoPool}
import net.liftweb.json.JsonAST.JValue
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import scala.collection.mutable.{Set => MSet, Map => MMap, ListBuffer, ArrayBuffer}
import scala.reflect.runtime.universe._
import scala.tools.nsc.Settings
import scala.tools.reflect.ToolBox
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import net.liftweb.json.Serialization.read


/**
  * Created by dimberman on 4/26/16.
  */
class LoadBalancerTest extends FlatSpec with Matchers with BeforeAndAfter {
    type Key = (Int, Int)

    implicit val formats = DefaultFormats


    val testNeededVecs = Array((0, 0), (1, 0), (1, 1), (2, 1), (2, 2), (3, 1), (4, 2))


    val testBucketSizes = Map(((1, 0), 10), ((2, 0), 30), ((3, 0), 41), ((4, 0), 40)).mapValues(_.toLong)


    val testLoad: Map[Key, List[Key]] = Map(
        ((1, 0), List()),
        ((2, 0), List()),
        ((3, 0), List((2, 0))),
        ((4, 0), List((1, 0), (2, 0), (3, 0)))
    )
    val testRefinementLoad: MMap[Key, MSet[Key]] = MMap() ++ Map(
        ((1, 0), MSet[Key]() ++ Set((4, 0))),
        ((2, 0), MSet[Key]() ++ Set((3, 0), (4, 0))),
        ((3, 0), MSet[Key]() ++ Set((4, 0))),
        ((4, 0), MSet[Key]() ++ Set())
    )

    val scaleBucketSizes = Map((7, 1) -> 3037, (7, 5) -> 14735, (19, 9) -> 114, (15, 7) -> 75, (10, 3) -> 1039, (20, 16) -> 1892, (16, 8) -> 96, (17, 9) -> 38, (5, 0) -> 12130, (10, 5) -> 47, (11, 7) -> 10205, (11, 6) -> 10, (15, 2) -> 21541, (9, 0) -> 10943, (16, 5) -> 8830, (18, 5) -> 6917, (17, 2) -> 9820, (20, 18) -> 346, (0, 0) -> 47620, (14, 14) -> 13, (11, 5) -> 610, (14, 1) -> 6448, (5, 2) -> 25, (13, 5) -> 1851, (18, 13) -> 2161, (11, 1) -> 20148, (7, 4) -> 15912, (5, 1) -> 498, (4, 0) -> 8454, (18, 1) -> 1934, (12, 1) -> 8988, (6, 4) -> 18714, (14, 10) -> 13355, (20, 17) -> 952, (7, 7) -> 398, (14, 4) -> 3487, (13, 0) -> 2877, (14, 9) -> 3, (15, 9) -> 3, (16, 6) -> 1351, (12, 5) -> 401, (19, 8) -> 253, (6, 6) -> 1696, (15, 1) -> 1076, (15, 11) -> 5105, (3, 1) -> 28860, (9, 1) -> 6832, (15, 0) -> 1937, (13, 1) -> 12871, (6, 1) -> 1100, (4, 1) -> 117, (14, 0) -> 2583, (18, 4) -> 10568, (13, 2) -> 14826, (19, 7) -> 2149, (11, 4) -> 1564, (6, 2) -> 115, (20, 11) -> 7, (19, 6) -> 6680, (16, 0) -> 1770, (8, 1) -> 7277, (2, 0) -> 14323, (15, 8) -> 26, (17, 5) -> 9449, (4, 4) -> 1054, (3, 0) -> 4185, (17, 10) -> 8, (12, 2) -> 13772, (18, 8) -> 205, (8, 0) -> 17466, (19, 2) -> 3293, (20, 1) -> 686, (14, 6) -> 94, (20, 2) -> 744, (10, 10) -> 179, (10, 1) -> 20835, (13, 9) -> 7975, (16, 9) -> 20, (14, 2) -> 15177, (12, 9) -> 8695, (20, 8) -> 773, (11, 3) -> 2719, (1, 1) -> 4382, (6, 3) -> 8976, (20, 5) -> 3663, (14, 3) -> 5085, (16, 12) -> 7543, (13, 13) -> 29, (7, 3) -> 211, (20, 0) -> 52, (19, 3) -> 10803, (16, 1) -> 1275, (8, 3) -> 723, (10, 7) -> 11362, (11, 8) -> 935, (14, 5) -> 1324, (17, 13) -> 2653, (20, 19) -> 23, (10, 2) -> 6635, (8, 2) -> 7781, (14, 8) -> 24, (17, 7) -> 416, (12, 8) -> 8368, (12, 7) -> 5, (17, 4) -> 6924, (11, 2) -> 4606, (19, 4) -> 12690, (8, 8) -> 721, (19, 10) -> 28, (18, 11) -> 1, (19, 11) -> 2, (13, 4) -> 3503, (12, 3) -> 1958, (18, 9) -> 121, (17, 1) -> 1640, (15, 15) -> 17, (19, 14) -> 1767, (14, 7) -> 26, (19, 5) -> 4799, (15, 5) -> 6822, (3, 2) -> 12758, (10, 6) -> 2, (20, 3) -> 8906, (18, 14) -> 1984, (11, 11) -> 184, (2, 2) -> 3536, (5, 5) -> 1430, (16, 16) -> 7, (20, 4) -> 12749, (4, 2) -> 37994, (20, 7) -> 5557, (13, 6) -> 30, (16, 2) -> 11573, (18, 3) -> 11298, (13, 7) -> 13, (15, 3) -> 6645, (17, 6) -> 2101, (17, 0) -> 1633, (20, 9) -> 109, (12, 12) -> 19, (12, 4) -> 1497, (10, 4) -> 1370, (9, 6) -> 25031, (8, 4) -> 165, (12, 6) -> 21, (16, 4) -> 5590, (5, 3) -> 33536, (17, 3) -> 12601, (18, 2) -> 5283, (20, 10) -> 26, (17, 8) -> 336, (3, 3) -> 1816, (19, 1) -> 1619, (20, 6) -> 11134, (13, 3) -> 3637, (10, 0) -> 6150, (19, 0) -> 473, (18, 0) -> 947, (2, 1) -> 29760, (15, 6) -> 444, (9, 9) -> 635, (6, 0) -> 17018, (15, 4) -> 3928, (18, 6) -> 5457, (8, 5) -> 13486, (16, 7) -> 127, (9, 3) -> 1401, (9, 4) -> 236, (18, 7) -> 731, (7, 2) -> 1259, (18, 10) -> 12, (11, 0) -> 6638, (9, 5) -> 22, (19, 15) -> 2949, (13, 8) -> 7, (1, 0) -> 43237, (12, 0) -> 3895, (9, 2) -> 2519, (7, 0) -> 12067, (16, 3) -> 9437).mapValues(_.toLong)


    "assignByBucket" should "match every correct pair only once" in {
        val assigned = testNeededVecs.map { case (b, l) => ((b, l), LoadBalancer.assignByBucket(b, l, 5, testNeededVecs)) }
          .flatMap { case (a, b) => b.map(c => if (c._1 > a._1 || c._1 == a._1 && c._2 > a._2) (c, a) else (a, c)) }.sorted

        val expected = Array(
            ((0, 0), (0, 0)),
            ((1, 0), (1, 0)),
            ((1, 1), (0, 0)),
            ((1, 1), (1, 0)),
            ((1, 1), (1, 1)),
            ((2, 1), (2, 1)),
            ((2, 2), (0, 0)),
            ((2, 2), (1, 0)),
            ((2, 2), (1, 1)),
            ((2, 2), (2, 1)),
            ((2, 2), (2, 2)),
            ((3, 1), (2, 1)),
            ((3, 1), (2, 2)),
            ((3, 1), (3, 1)),
            ((4, 2), (3, 1)),
            ((4, 2), (4, 2))


        )

        assigned.foreach(println)
        assigned.length shouldEqual expected.length

        assigned should contain allElementsOf expected


    }



    "initialLoadAssignment" should "redistribute in a greedy fashion" in {
        val testInput = MMap() ++ testLoad.mapValues(MSet() ++ _.toSet).map(identity)

        val answer = LoadBalancer.initialLoadAssignment(testInput, testBucketSizes)
        val expected = MMap() ++ Map(
            ((1, 0), MSet((4, 0))),
            ((2, 0), MSet((3, 0), (4, 0))),
            ((3, 0), MSet((4, 0))),
            ((4, 0), MSet())
        )
        println(answer.mkString("\n"))
        answer should contain allElementsOf expected
    }


    "loadRefinement" should "fix errors in the greedy algorithm" in {


        val answer = LoadBalancer.loadAssignmentRefinement(testRefinementLoad, testBucketSizes)
        val expected = MMap() ++ Map(
            ((1, 0), MSet((4, 0))),
            ((2, 0), MSet((3, 0))),
            ((3, 0), MSet((4, 0))),
            ((4, 0), MSet((2, 0)))
        )
        println(answer.mkString("\n"))
        answer should contain allElementsOf expected
    }





//
//    "loadRefinement" should "work at scale" in {
//        //        val scaleBucketSizes = scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("scaleMap"))
//        //                  .getLines().toList.head
////        val scaleValues = scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("scaleValues"))
////          .getLines().toList
//        //        val settings = new Settings
//        //        settings.usejavacp.value = true
//        var g = 0
//
//        val inp = List(
//            ((2, 1), Set((15, 15), (4, 4), (9, 1), (3, 1), (17, 0), (16, 16), (13, 0), (19, 1), (9, 0), (6, 6), (8, 8), (15, 0), (10, 10), (11, 1), (4, 1), (20, 1), (14, 0), (9, 9), (5, 5), (13, 13), (15, 1), (7, 1), (17, 1), (7, 7), (20, 0), (5, 1), (6, 1), (13, 1), (10, 1), (2, 1), (16, 1), (14, 14), (2, 2), (16, 0), (7, 0), (3, 0), (12, 0), (3, 3), (12, 12), (8, 1), (12, 1), (18, 1), (19, 0), (8, 0), (5, 0), (18, 0), (11, 11))),
//            ((11, 11), Set((4, 4), (15, 6), (20, 5), (17, 5), (19, 1), (16, 4), (19, 6), (12, 5), (15, 5), (15, 0), (16, 6), (20, 7), (18, 5), (14, 0), (11, 8), (5, 5), (19, 2), (7, 1), (7, 7), (13, 3), (20, 6), (19, 5), (9, 3), (5, 1), (9, 2), (18, 6), (6, 1), (17, 6), (15, 4), (0, 0), (13, 4), (3, 3), (8, 1), (14, 4), (18, 1), (18, 2), (18, 0), (11, 11), (11, 2), (10, 4), (9, 1), (18, 4), (17, 0), (16, 7), (6, 3), (13, 0), (17, 4), (10, 3), (8, 8), (6, 6), (14, 3), (1, 1), (12, 3), (13, 5), (9, 9), (15, 1), (17, 1), (11, 5), (14, 5), (7, 2), (16, 1), (15, 3), (20, 3), (16, 0), (2, 2), (17, 7), (3, 0), (12, 0), (12, 4), (16, 5), (19, 7), (11, 3), (18, 7), (11, 4), (8, 3))),
//            ((17, 10), Set((15, 15), (15, 6), (20, 5), (17, 5), (18, 10), (16, 16), (18, 14), (19, 1), (18, 9), (19, 6), (12, 5), (15, 5), (16, 4), (15, 0), (15, 11), (17, 8), (16, 12), (16, 6), (11, 1), (20, 7), (18, 5), (14, 0), (17, 13), (11, 8), (13, 13), (19, 2), (18, 11), (15, 7), (13, 3), (20, 6), (19, 5), (20, 11), (18, 6), (14, 8), (14, 6), (17, 10), (19, 15), (17, 9), (17, 6), (18, 13), (14, 14), (20, 9), (15, 4), (13, 4), (12, 12), (19, 10), (13, 6), (16, 9), (14, 4), (18, 1), (18, 2), (18, 0), (11, 11), (20, 16), (11, 2), (18, 4), (17, 0), (12, 2), (16, 7), (11, 6), (17, 4), (13, 0), (20, 8), (14, 3), (13, 7), (12, 3), (18, 8), (20, 1), (19, 14), (13, 5), (15, 8), (13, 8), (12, 7), (16, 8), (15, 1), (17, 1), (19, 8), (19, 9), (20, 0), (14, 2), (11, 5), (14, 5), (16, 1), (15, 3), (20, 3), (16, 0), (17, 7), (12, 0), (12, 4), (16, 5), (14, 7), (19, 7), (18, 7), (11, 3), (12, 6), (19, 0), (11, 4), (20, 2))),
//            ((9, 6), Set((15, 15), (20, 5), (17, 5), (9, 0), (16, 4), (15, 0), (20, 7), (14, 0), (12, 8), (19, 2), (10, 7), (7, 1), (15, 7), (7, 7), (13, 3), (19, 5), (19, 4), (13, 1), (18, 6), (18, 3), (14, 6), (17, 6), (14, 14), (15, 4), (13, 4), (8, 4), (10, 5), (14, 4), (18, 1), (18, 2), (18, 0), (11, 11), (11, 2), (9, 1), (11, 6), (18, 4), (16, 7), (17, 4), (10, 3), (20, 8), (9, 5), (13, 7), (13, 5), (13, 8), (15, 1), (19, 8), (20, 0), (14, 2), (11, 5), (14, 5), (7, 2), (16, 1), (16, 0), (7, 0), (12, 0), (12, 1), (17, 2), (19, 0), (11, 4), (20, 2), (8, 0), (7, 3), (15, 6), (16, 16), (19, 1), (19, 6), (12, 5), (15, 5), (10, 10), (11, 7), (17, 8), (16, 6), (11, 1), (18, 5), (11, 8), (13, 13), (10, 2), (8, 2), (20, 6), (9, 3), (20, 4), (9, 2), (14, 8), (12, 12), (8, 1), (13, 6), (10, 4), (17, 0), (12, 2), (13, 0), (13, 2), (8, 8), (14, 3), (12, 3), (18, 8), (20, 1), (15, 8), (9, 9), (12, 7), (16, 8), (17, 1), (10, 6), (17, 3), (9, 4), (10, 1), (15, 3), (20, 3), (8, 5), (17, 7), (12, 4), (16, 5), (14, 7), (19, 7), (11, 3), (18, 7), (12, 6), (16, 2), (8, 3), (15, 2), (9, 6)))
//
//        ).map(createStringTupleList)
//
//        inp.foreach{
//           x =>
//               println(write(x))
//        }
//
//
////
////        scaleValues.foreach {
////            i =>
////                val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()
////                val evaluated = toolbox.eval(toolbox.parse(i)).asInstanceOf[((Int, Int), Set[(Int, Int)])]
////                val parsed = createStringTupleList(evaluated)
////                val x = write(parsed)
////                //                println(g +": " + x)
////                g += 1
////
////        }
////
////        //        val mJson = write(parsed)
////        //        val mJson = write(parsed.toList)
////        //                val input = List(((1,1),Set(1)),((2,2),Set(2)))
////
////        val input = List(((1, 2), Set((1, 3), (4, 5))), ((2, 3), Set((2, 5), (6, 7))))
////
////        val parsed = input.map(createStringTupleList)
////
////        //        val kryo = KryoPool.withByteArrayOutputStream(100, new KryoInstantiator())
////        //        val b = kryo.toBytesWithoutClass(input)
////        //        val s = b.map(_.toChar).toString
////        //        val deserialized = kryo.fromBytes(s.toCharArray.map(_.toByte))
////        //        val serialized =
////
////        //        scaleValues.foreach(
////        //            s =>
////        //        val p = write(c)
////        //        )
////
////
////        val x = write(parsed)
////        val y = read[List[(String, String)]](x)
////        //
////        y.map(convertBackToOjbect) shouldEqual input
//
//
//    }


        "loadRefinement" should "work at scale" in {
            //        val scaleBucketSizes = scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("scaleMap"))
            //                  .getLines().toList.head
            val scaleValues = scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("scaleValues"))
              .getLines().toList
            //        val settings = new Settings
            //        settings.usejavacp.value = true
            val inp = MMap() ++ scaleValues.map{
                i =>
                    val y = read[(String,String)](i)
                    convertBackToOjbect(y)
            }

            LoadBalancer.loadAssignmentRefinement(inp, scaleBucketSizes)



        }


    def createStringTupleList(input: ((Int, Int), Set[(Int, Int)])) = {
        val (k, v) = (input._1, input._2)
        (k._1 + ":" + k._2, v.toArray.map(x => x._1 + ":" + x._2).mkString("%"))
    }

    def convertBackToOjbect(input: (String, String)): ((Int, Int), MSet[(Int, Int)]) = {

        val (k, v) = (input._1, input._2)
        (createTuple(k), MSet() ++ v.split("%").map(createTuple).toSet)
    }

    def createTuple(inp: String) = {
        val x = inp.split(":").map(_.toInt)
        (x.head, x.tail.head)
    }
}
