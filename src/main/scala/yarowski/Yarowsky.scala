/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.yarowski

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable


class YarowskyConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object Yarowsky extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  // def read(input:RDD[String], sc:SparkContext, model_path:String, result_path: String): Double={
  //   val tr = sc.textFile(path)
  //   return tr
  // }

  /**
  extract N-Gram as a map from String to int, for example: "A B C" -> 1 (3-gram)
  */
  def extractNGram(positives:RDD[String], negatives:RDD[String], N:Int, m:Int, ranking:String):Map[String, Int]={
    val o = positives.union(negatives)
    .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length >= N) tokens.sliding(N).map(p => p.mkString(" ")).toList else List()
      })

    println("################### N-Gram ##################")
    /////test

    o.take(5).foreach(println)

    return Map[String, Int]()
  }

/**
  run Yarowsky's Algorithm with fixed hyper parameters
*/
  def run(positives:RDD[String], negatives:RDD[String], unclassified:RDD[String], sc:SparkContext, model_path:String, result_path: String): Double={
    val tr = extractNGram(positives,negatives,2,100,"None")
    return 0.0
  }
  // def extractNGram(n:Int,)

  def main(argv: Array[String]): Unit = {
    val args = new YarowskyConf(argv)

    val conf = new SparkConf().setAppName("Bigram Count")
    // conf.set("spark.sql.shuffle.partitions", args.reducers()+"")
    // conf.set("spark.default.parallelism", args.reducers()+"")
    val sc = new SparkContext(conf)
    
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    /*
    split input into positives, negatives and unclassified
    */
    val positives = textFile.filter(s=>s.slice(s.size-1,s.size)=="+")
    .map(s=>s.slice(0,s.size-1))

    val negatives = textFile.filter(s=>s.slice(s.size-1,s.size)=="-")
    .map(s=>s.slice(0,s.size-1))

    val unclassified = textFile.filter(s=>s.slice(s.size-1,s.size)=="0")
    .map(s=>s.slice(0,s.size-1))

    ///// test
    println("################### positives ##################")
    positives.foreach(println)
    println("################### negatives ##################")
    negatives.foreach(println)
    println("################### unclassified ##################")
    unclassified.take(5).foreach(println)

    /*
    test run
    */
    val model_path = "model"
    val result_path = "result"

    val acc = run(positives, negatives, unclassified, sc, model_path, result_path)

    // val textFile2 = read(textFile, sc, args.input())
    // val tokens = textFile.map(line => tokenize(line))
    
    // val bCount = tokens.flatMap(ts =>{
    //   if (ts.length > 1) ts.sliding(2).map(p => {
    //        (p(0),p(1))
    //        }).toList else List()
    //   })
    // .map(key=> (key, 1)).reduceByKey(_ + _)
    
    // val sCount = bCount.map(bc => (bc._1._1, bc._2)).reduceByKey(_+_)
    // val freq = bCount.map(p => (p._1._1,p)).join(sCount)
    // .map(p=>(p._2._1._1,p._2._1._2.toDouble/p._2._2.toDouble))

    // textFile2.saveAsTextFile(args.output())
  }
}
