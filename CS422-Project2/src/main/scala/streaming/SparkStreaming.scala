package streaming;

import scala.io.Source
import scala.util.control._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming._

import scala.collection.{Seq, mutable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.Strategy
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.net.URI
import java.util
import java.util.Comparator

import javafx.collections.transformation.SortedList
import org.apache.spark.util.sketch.CountMinSketch

import scala.collection.immutable.HashSet
import scala.util.Random

class SparkStreaming(sparkConf: SparkConf, args: Array[String]) {

  val sparkconf = sparkConf;

  // get the directory in which the stream is filled.
  val inputDirectory = args(0)

  // number of seconds per window
  val seconds = args(1).toInt;

  // K: number of heavy hitters stored
  val TOPK = args(2).toInt;

  // precise Or approx
  val execType = args(3);

  //  create a StreamingContext, the main entry point for all streaming functionality.
  val ssc = new StreamingContext(sparkConf, Seconds(seconds));

  val checkpointDirectory = args(4)

  val d:Int = args.length match {
    case 7 => args(5).toInt
    case _ => 0
  }
  val w:Int = args.length match {
    case 7 => args(6).toInt
    case _ => 0
  }

  def consume() {
    ssc.sparkContext.setCheckpointDir(checkpointDirectory)
    val topk = ssc.sparkContext.broadcast(TOPK)
    val d_b = ssc.sparkContext.broadcast(d)
    val w_b = ssc.sparkContext.broadcast(w)

    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(inputDirectory);

    // parse the stream. (line -> (IP1, IP2))
    val words = linesDStream.map(x => (x.split("\t")(0), x.split("\t")(1)))

    if (execType.contains("precise")) {
      val preciseUpdate = (new_values: Seq[((String, String),Int)], running_state: Option[(mutable.HashMap[(String,String), Int], String, String)]) => {
        val state = running_state.getOrElse((new mutable.HashMap[(String,String), Int], "", ""))

        val batch_topk = new_values.sortWith((a, b) => a._2 > b._2).take(topk.value).toArray
        for (value <- new_values) {
          state._1.put(value._1, state._1.getOrElse(value._1, 0) + value._2)
        }
        val global_topk = state._1.toArray.sortWith((a, b) => a._2 > b._2).take(topk.value)

        val print = (prefix: String, arr: Array[((String, String), Int)]) => {
          var arr_str = StringBuilder.newBuilder
          arr_str = arr.map(a => String.format("(%s,(%s,%s))", a._2.toString, a._1._1, a._1._2))
            .addString(arr_str, prefix + ": [", ",", "]")

          arr_str.result()
        }

        Some(state._1, print("This batch", batch_topk), print("Global", global_topk))
      }

      words.map(word => (word, 1))
        .reduceByKey((a, b) => a+b)
        .map(elem => (0, elem))
        .updateStateByKey(preciseUpdate)
        .flatMap(s => List(s._2._2, s._2._3))
        .print(2)
    } else if (execType.contains("approx")) {

      val approxUpdate = (new_values: Seq[((String,String), Int)], running_state: Option[(mutable.Set[(String, String)], Array[Array[Int]], String, String)]) => {
        val state = running_state.getOrElse((new mutable.HashSet[(String, String)], Array.ofDim[Int](d_b.value, w_b.value), "", ""))
        val seen = state._1++new_values.map(v => v._1)

        val r = Random
        r.setSeed(2018)
        var hashes = List[Int => Int]()
        for (_ <- 0 until d_b.value) {
          hashes = hashes :+ ((x:Int) => math.abs(math.abs(Murmur3HashFunction.hash(x, null, r.nextLong()).toInt) % w_b.value))
        }

        // Update the count matrix
        for (word <- new_values) {
          for (i <- 0 until d_b.value) {
            val hash = hashes(i)((word._1._1 + word._1._2).hashCode)
            state._2(i)(hash) = state._2(i)(hash) + word._2
          }
        }

        // Calculate batch top-k
        val batch_topk = new_values.sortWith((a, b) => a._2 > b._2).take(topk.value).toArray

        // Approximate global top-k
        var global_approx = new Array[((String,String), Int)](0)
        for (s <- seen) {
          var min = Int.MaxValue
          for (i <- 0 until d_b.value) {
            val hash = hashes(i)((s._1+s._2).hashCode)
            if (state._2(i)(hash) < min) {
              min = state._2(i)(hash)
            }
          }
          global_approx = global_approx :+ (s, min)
        }
        val global_topk = global_approx.sortWith((a, b) => a._2 > b._2).take(topk.value)

        val print = (prefix: String, arr: Array[((String, String), Int)]) => {
          var arr_str = StringBuilder.newBuilder
          arr_str = arr.map(a => String.format("(%s,(%s,%s))", a._2.toString, a._1._1, a._1._2))
            .addString(arr_str, prefix + ": [", ",", "]")

          arr_str.result()
        }

        Some(seen, state._2, print("This batch", batch_topk), print("Global", global_topk))
      }

      words.map(word => (word, 1))
        .reduceByKey((a, b) => a+b)
        .map{ word => (0, word) }
        .updateStateByKey(approxUpdate)
        .flatMap(s => List(s._2._3, s._2._4))
        .print(2)
    }

    // Start the computation
    ssc.start()

    // Wait for the computation to terminate
    ssc.awaitTermination()
  }

  class State {
    var batch:String = ""
    var global:String = ""
    var seen: Set[(String, String)] = new HashSet[(String, String)]
    var mat: Array[Array[Int]] = Array.ofDim[Int](d, w)
  }
}