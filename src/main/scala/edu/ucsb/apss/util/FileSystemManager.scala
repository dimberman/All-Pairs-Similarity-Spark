package edu.ucsb.apss.util

import java.io.{BufferedReader, InputStreamReader, File}

import edu.ucsb.apss.InvertedIndex.{InvertedIndex, SimpleInvertedIndex}
import edu.ucsb.apss.PSS.Similarity
import edu.ucsb.apss.partitioning.PartitionHasher
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SerializableWritable, SparkEnv, TaskContext}

/**
  * Created by dimberman on 4/14/16.
  */
case class FileSystemManager(local: Boolean = false, outputDir: String = "") extends Serializable {


    def readVecPartition(key: (Int, Int), id: String, broadcastedConf: Broadcast[SerializableWritable[Configuration]], taskContext: TaskContext): Iterator[VectorWithNorms] = {
        val partitionFile = s"/tmp/$id/" + PartitionHasher.partitionHash(key)
        readVecFile(new Path(partitionFile), broadcastedConf, taskContext)
        //         List[VectorWithNorms]().toIterator
    }

    def readVecFile(path: Path, broadcastedConf: Broadcast[SerializableWritable[Configuration]], context: TaskContext) = {
        val env = SparkEnv.get
        val fs = path.getFileSystem(broadcastedConf.value.value)
        val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
        val fileInputStream = fs.open(path, bufferSize)
        val serializer = env.serializer.newInstance()

        val deserializeStream = serializer.deserializeStream(fileInputStream)
        //        context.addTaskCompletionListener(context => deserializeStream.close())
        deserializeStream.asIterator.asInstanceOf[Iterator[VectorWithNorms]]
    }


    def readInvPartition(key: (Int, Int), id: String, broadcastedConf: Broadcast[SerializableWritable[Configuration]], taskContext: TaskContext): Iterator[SimpleInvertedIndex] = {
        val partitionFile = s"/tmp/$id/" + PartitionHasher.partitionHash(key)
        readInvFile(new Path(partitionFile), broadcastedConf, taskContext)
        //         List[VectorWithNorms]().toIterator
    }

    def readInvFile(path: Path, broadcastedConf: Broadcast[SerializableWritable[Configuration]], context: TaskContext) = {
        val env = SparkEnv.get
        val fs = path.getFileSystem(broadcastedConf.value.value)
        val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
        val fileInputStream = fs.open(path, bufferSize)
        val serializer = env.serializer.newInstance()

        val deserializeStream = serializer.deserializeStream(fileInputStream)
        //        context.addTaskCompletionListener(context => deserializeStream.close())
        deserializeStream.asIterator.asInstanceOf[Iterator[SimpleInvertedIndex]]
    }

    def getSums(i: Int): Array[Int] = {
        val ret = new Array[Int](i + 1)
        ret(0) = 1
        for (j <- 1 to i) {
            ret(j) = ret(j - 1) + j + 1
        }
        ret
    }

    def writePartitionsToFile(r: RDD[((Int, Int), VectorWithNorms)]) = {
        val x = r.groupByKey()
        val id = r.context.applicationId
        val BVConf = r.context.broadcast(new SerializableWritable(r.context.hadoopConfiguration))

        x.foreach { case (k, v) =>
            writeVecFileToInvertedIndexes(k, v, id, BVConf)
        }
        val y = x.collect

    }

    def writeInvertedIndexesToFile(r: RDD[((Int, Int), Iterable[SimpleInvertedIndex])]) = {
        val id = r.context.applicationId
        val BVConf = r.context.broadcast(new SerializableWritable(r.context.hadoopConfiguration))
        r.foreach { case (k, v) =>
            writeInvFile(k, v, id, BVConf)
        }
        val y = r.collect

    }

    def writeVecFileToInvertedIndexes(key: (Int, Int), f: Iterable[VectorWithNorms], id: String, BVConf: Broadcast[SerializableWritable[Configuration]]) = {
        val partitionFile = s"/tmp/$id/" + PartitionHasher.partitionHash(key)
        val path = new Path(partitionFile)
        val fs = path.getFileSystem(BVConf.value.value)

        val invertedIndexes = f.grouped(32).map(a => InvertedIndex.extractFeaturePairs(a.toList))


        val env = SparkEnv.get
        val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
        if (!fs.exists(path)) {
            val output = fs.create(path, false, bufferSize)
            val serialized = env.serializer.newInstance().serializeStream(output)
            serialized.writeAll(invertedIndexes)
            output.close()
        }
    }


    //    def writePartitionListsToFile(r: RDD[((Int, Int), List[List[VectorWithNorms]])]) = {
    //        val x = r.groupByKey()
    //        val id = r.context.applicationId
    //        val BVConf = r.context.broadcast(new SerializableWritable(r.context.hadoopConfiguration))
    //        x.foreach { case (k, v) =>
    //            writeFile(k, v, id,BVConf)
    //            1
    //        }
    //        val y = x.collect
    //
    //    }


    def cleanup(id: String, BVConf: Broadcast[SerializableWritable[Configuration]]) = {
        val file = s"/tmp/$id/"
        val path = new Path(file)
        val fs = path.getFileSystem(BVConf.value.value)
        fs.delete(path, true)

    }

    def writeVecFile(key: (Int, Int), f: Iterable[VectorWithNorms], id: String, BVConf: Broadcast[SerializableWritable[Configuration]]) = {
        val partitionFile = s"/tmp/$id/" + PartitionHasher.partitionHash(key)
        val path = new Path(partitionFile)
        val fs = path.getFileSystem(BVConf.value.value)

        val env = SparkEnv.get
        val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
        if (!fs.exists(path)) {
            val output = fs.create(path, false, bufferSize)
            val serialized = env.serializer.newInstance().serializeStream(output)
            serialized.writeAll(f.toIterator)
            output.close()
        }


    }


    def genOutputStream(key: (Int, Int), BVConf: Broadcast[SerializableWritable[Configuration]]) = {
        val hashedKey = PartitionHasher.partitionHash(key)
        val partitionFile = s"$outputDir/" + hashedKey
        val path = new Path(partitionFile)
        val fs = path.getFileSystem(BVConf.value.value)
        val env = SparkEnv.get
        val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
        fs.create(path, false, bufferSize)
    }


    def writeSimilaritiesToFile(f: Seq[Similarity], output: FSDataOutputStream) = {

        for (s <- f) {
            val out = s.toString + "\n"
            output.writeBytes(out)
        }

    }


    def writeInvFile(key: (Int, Int), f: Iterable[SimpleInvertedIndex], id: String, BVConf: Broadcast[SerializableWritable[Configuration]]) = {
        val partitionFile = s"/tmp/$id/" + PartitionHasher.partitionHash(key)
        val path = new Path(partitionFile)
        val fs = path.getFileSystem(BVConf.value.value)

        val env = SparkEnv.get
        val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
        if (!fs.exists(path)) {
            val output = fs.create(path, false, bufferSize)
            //            println(s"writing vector to file $id: " + f)
            val serialized = env.serializer.newInstance().serializeStream(output)
            serialized.writeAll(f.toIterator)
            output.close()
        }

    }

}