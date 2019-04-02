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
  def extractNGram(sents:RDD[(String,Int)], N:Int, m:Int, tf_idf:Boolean):scala.collection.Map[String, Int]={
    ///// no tf-idf
    val res = sents.filter(s=>s._2!=0)
    .flatMap(line => {
        val tokens = tokenize(line._1)
        if (tokens.length >= N) tokens.sliding(N).map(p => p.mkString(" ")).toList else List()
      })
    .distinct
    .zipWithIndex
    .map(t=>(t._1,t._2.toInt))
    .filter(t=> t._2 < m)
    .collectAsMap()

    /////test
    // println("################### N-Gram ##################")
    // res.take(5).foreach(println)

    return res
  }

  /**
  convert sentences to list of n-gram features
  */
  def toNGram(sents:RDD[(String,Int)], f_map:scala.collection.Map[String, Int], N:Int): RDD[(List[Int],Int)]={
    val res = sents.map(line=>{
      val tokens = tokenize(line._1)
      val lst = 
      if (tokens.length >= N)
        tokens.sliding(N)
        .map(p => p.mkString(" "))
        .filter(p=>f_map.contains(p))
        .map(p=>f_map(p))
        .toList 
      else 
        List[Int]()
      (lst,line._2)
      })

    return res
  }

  // def train(f_positives:RDD[List(Int)], f_negatives:RDD[List(Int)], n_iter:Int):scala.collection.Map[Int, Double]={
  //   var iter = 0
  //   for(iter <- 0 to n_iter){

  //   }
  // }

/**
  run Yarowsky's Algorithm with fixed hyper parameters
*/
  def run(sents:RDD[(String,Int)], sc:SparkContext, model_path:String, result_path: String,
    N:Int, m:Int, tf_idf:Boolean, n_iter:Int): Double={
    // var n_unclassified = 0
    // var n_unclassified_new = unclassified.count()

    // while(n_unclassified_new>0 && n_unclassified_new!=n_unclassified){
      val f_map = extractNGram(sents,N,m,tf_idf)
      val f_sents = toNGram(sents, f_map, N)

      /////test
      println("################### converted N-Gram ##################")
      f_sents.take(5).foreach(println)

    //   n_unclassified = n_unclassified_new

    // }

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
    val sents = textFile.map(s=>(s.slice(0,s.size-1), s.slice(s.size-1,s.size)))
    .map(t=>(t._1,if(t._2=="+") 1 else (if(t._2=="-") -1 else 0)))


    // val positives = textFile.filter(s=>s.slice(s.size-1,s.size)=="+")
    // .map(s=>s.slice(0,s.size-1))

    // val negatives = textFile.filter(s=>s.slice(s.size-1,s.size)=="-")
    // .map(s=>s.slice(0,s.size-1))

    // val unclassified = textFile.filter(s=>s.slice(s.size-1,s.size)=="0")
    // .map(s=>s.slice(0,s.size-1))

    ///// test
    // println("################### sents ##################")
    // sents.foreach(println)

    /*
    test run
    */
    val model_path = "model"
    val result_path = "result"

    val acc = run(sents, sc, model_path, result_path,
      2, 10000, true, 100)

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
