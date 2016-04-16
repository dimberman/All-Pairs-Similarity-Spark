package edu.ucsb.apss.partitioning

import java.io.File
import java.nio.file.{Files, Paths}

import edu.ucsb.apss.Context
import edu.ucsb.apss.util.PartitionUtil.VectorWithNorms
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SerializableWritable
import org.apache.spark.mllib.linalg.SparseVector
import org.scalatest.{Matchers, BeforeAndAfter, FlatSpec}

/**
  * Created by dimberman on 4/14/16.
  */
class PartitionManagerTest extends FlatSpec with Matchers with BeforeAndAfter {

    val manager = new PartitionManager
    val sc = Context.sc
    val path = (s:String, i:(Int,Int)) => s"/tmp/$s/${PartitionHasher.partitionHash(i)}"

    val k = (5, 5)
    val bucketizedVector = Seq( new VectorWithNorms(1, 1, 1, new SparseVector(1, Array(2), Array(3)), 1)).toIterable

    "Partition Manager" should "write to file" in {

        manager.writeFile(k, bucketizedVector, sc.applicationId, sc.broadcast(new SerializableWritable(sc.hadoopConfiguration)))

        assert(Files.exists(Paths.get(path(sc.applicationId, k))))
        val f = new File(s"/tmp/${sc.applicationId}/"+PartitionHasher.partitionHash(k))
        f.delete()
    }

    it should "read from a file it wrote" in {
        manager.writeFile(k, bucketizedVector, sc.applicationId, sc.broadcast(new SerializableWritable(sc.hadoopConfiguration)))

        assert(Files.exists(Paths.get(s"/tmp/${sc.applicationId}/"+PartitionHasher.partitionHash(k))))

        val BVConf = sc.broadcast(new SerializableWritable(sc.hadoopConfiguration))

        Files.exists(Paths.get(path(sc.applicationId, k))) shouldBe true

        val answer = manager.readFile[VectorWithNorms](new Path(path(sc.applicationId, k)),BVConf, org.apache.spark.TaskContext.get())
        answer.next() shouldEqual bucketizedVector.head
        val f = new File(s"/tmp/${sc.applicationId}/"+PartitionHasher.partitionHash(k))
        f.delete()
    }


    it should "handle RDDs" in {
        val rdd = sc.parallelize(Seq((k,bucketizedVector.head)))
        manager.writePartitionsToFile(rdd)
        rdd.count()
        assert(Files.exists(Paths.get(path(sc.applicationId, k))))
        val f = new File(s"/tmp/${sc.applicationId}/"+PartitionHasher.partitionHash(k))
        f.delete()
    }

}
