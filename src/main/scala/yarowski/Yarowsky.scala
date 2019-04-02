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
import scala.math._
import scala.util.control.Breaks._

import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
config: ignore
*/
class YarowskyConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object Yarowsky extends Tokenizer {
  // ignore
  val log = Logger.getLogger(getClass().getName())

  /**
  extract N-Gram as a map from String to int, 
  for example: outputs {"A B C" -> 0 , "B C D" -> 1} (3-gram)
  */
  def extractNGram(sents:RDD[(String,Int)], N:Int, m:Int, tf_idf:Boolean):scala.collection.Map[String, Int]={
    ///// no tf-idf
    val res = sents.filter(s=>s._2!=0)//get only the classified sentences
    .flatMap(line => {//flatmap to n-grams, same as in assignment
        val tokens = tokenize(line._1)
        if (tokens.length >= N) tokens.sliding(N).map(p => p.mkString(" ")).toList else List()
      })
    .distinct
    .zipWithIndex//for example: {"a","b","c"} -> {("a",0),("b",1),("c",2)}
    .map(t=>(t._1,t._2.toInt))//type convert, ignore
    .filter(t=> t._2 < m)//only take m features (randomly), we can later pick top m with highest tf-idf
    .collectAsMap()//convert to dict, key is first element, value is the 2nd

    /////test
    // println("################### N-Gram ##################")
    // res.take(5).foreach(println)

    return res
  }

  /**
  convert String to a list of N-Gram features
  for example "A B C D" -> [0,1]
  */
  def toNGramSingle(sent:String, f_map:scala.collection.Map[String, Int], N:Int): List[Int]={
    //similar to assignment
    val tokens = tokenize(sent)//string to a list of words
    val lst = 
    if (tokens.length >= N)
      tokens.sliding(N)
      .map(p => p.mkString(" "))// extract N-Gram
      .filter(p=>f_map.contains(p))// if N-gram is not stored in f_map, ignore; f_map comes from extractNGram
      .map(p=>f_map(p))// find the number corresponding to the N-Gram
      .toList 
    else 
      List[Int]()

    return lst
  }

  /**
  convert sentences to list of n-gram features
  similar to toNGramSingle, but acts on an rdd.
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

/**
calculate the classification score of a sentence (a list of features)
same as the spamness function in assignment
*/
  def classify(features: List[Int], w: scala.collection.Map[Int, Double]):Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    return score
  }

/**
accuracy on a set of sentences
*/
  def accuracy(f_classified:RDD[(List[Int],Int)], w:scala.collection.Map[Int, Double]):Double={
    val corrects = f_classified.map(t=>("",if(classify(t._1,w)*t._2>0) 1D else 0D))// if the score and label have the same sign, add 1, else add 0
    .reduceByKey(_+_)// number of correctly classified = sum of the 1s and 0s
    .take(1)//it is an RDD of 1 row, get its first row, the result is a tuple: ("",number)
    return corrects(0)._2/f_classified.count()//accuracy = n_corrrect / n_total
  }

/**
train model
output a dict: {feature(int) -> weight(double)}, recall each n-gram is converted to a number (feature)
*/
  def train(f_classified:RDD[(List[Int],Int)], n_iter:Int, alpha: Double, delta: Double): scala.collection.Map[Int, Double]={
    var iter = 0//the current number of iteration

    var w = scala.collection.Map[Int, Double]()//init the dict to return
    breakable{//syntax, ignore
      for(iter <- 0 to n_iter){
        println("## trainning iter "+iter)
        /*
        this step calculates the gradiant for each feature
        example: f_classified = [
                  ([1,2],1) //suppose the classification score of this sentence = s1
                  ([2,3],-1) //score = s2
                  ]
        */
        val d_w = f_classified.flatMap(t => {
          val label = if(t._2>0) 1 else 0 // convert -1,1 label to 0,1 label
          val score = classify(t._1, w) // classification score
          val prob = 1.0 / (1 + exp(-score))
          t._1.map(f=>(f, (label - prob) * alpha))
          })// after this step, the emitted tuples are: (1,(1-1/(1+s1))*alpha), (2,(1-1/(1+s1))*alpha), (2,(-1-1/(1+s2))*alpha), (3,(-1-1/(1+s2))*alpha) 
        .reduceByKey(_+_)//after this step we have: (1,(1-1/(1+s1))*alpha), (2,(1-1/(1+s1))*alpha+(-1-1/(1+s2))*alpha), (3,(-1-1/(1+s2))*alpha) 

        w = d_w.map(t=>(t._1,t._2 + w.getOrElse(t._1,0.0)))//the new weights are the old weights + gradiants
        .collectAsMap()

        val s_d_w = d_w.map(t=>("",abs(t._2)))//how much the weights have changed in total
        .reduceByKey(_+_)
        .take(1)

        println("##  model weights changed by: " + s_d_w(0)._2)
        if(s_d_w(0)._2 < delta){//break if weights haven't changed much
          break
        }
      }
    }
    return w
  }

/**
  run Yarowsky's Algorithm with fixed hyper parameters
*/
  def run(sents0:RDD[(String,Int)], sc:SparkContext, model_path:String, result_path: String,
    N:Int, m:Int, tf_idf:Boolean, n_iter:Int, threshold:Double): Double={
    var n_unclassified = 0L
    var n_unclassified_new = sents0.filter(t => t._2 == 0).count()
    var iter = 0

    var sents = sents0//{(sentence, label)} label is 1,0 or -1

    while(n_unclassified_new>0 && n_unclassified_new != n_unclassified){//ends if all sentences are classified or no new sentences are classified in the last iteration

      println("# yarowsky iter " + iter)
      println("# extracting features")
      val f_map = extractNGram(sents,N,m,tf_idf)//{N-Gram -> number}
      val f_classified = toNGram(sents.filter(t=>t._2!=0), f_map, N)//filter the unclassified sents (ends with 0) and convert to list of features
      //for example: {([0,1],1),
      //              ([1,2],-1)}

      println("# trainning model")
      val w = train(f_classified, n_iter, 0.002, 0.05)// train model: {feature -> weight}
      val train_acc =  accuracy(f_classified, w)
      println("# trainning accuracy is: " + train_acc)

      println("# classifying new sents")
      val sents_new_0 = sents.map(t=>{// t is for example:("A B", 0)
        var label_new = t._2.toDouble
        if(t._2==0){// if label is 0, calculate the score
          val score = classify(toNGramSingle(t._1,f_map,N),w)
          label_new = score
        }
        (t._1, label_new)//for example:("A B", 0.2), 0.2 is the classification score
        })

      // println("# sentences newly classified:")
      // sents_new_0.filter(v=>v._2!=1&&v._2!=0-1&abs(v._2)>threshold).foreach(println)

      val sents_new = sents_new_0.map(t=>{//("A B", 0.2) as an example
        var lb = 0
        if(t._2>threshold){
          lb=1
        }
        else if(t._2 < 0-threshold){
          lb=0-1
        }
        (t._1,lb)//("A B", 1) if 0.2 > threshold
        })
      /////test
      // println("################### converted N-Gram ##################")
      // f_sents.take(5).foreach(println)

      n_unclassified = sents.filter(t => t._2 == 0).count()
      n_unclassified_new = sents_new.filter(t => t._2 == 0).count()
      println("# number of old sentences classified: "+n_unclassified)
      println("# number of new sentences classified: "+n_unclassified_new)

      iter = iter + 1
      sents = sents_new
    }

    return 0.0
  }
  // def extractNGram(n:Int,)

  def main(argv: Array[String]): Unit = {
    val args = new YarowskyConf(argv)

    val conf = new SparkConf().setAppName("Bigram Count")
    // conf.set("spark.sql.shuffle.partitions", args.reducers()+"")
    // conf.set("spark.default.parallelism", args.reducers()+"")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)
    
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //ignore all above this line
    val textFile = sc.textFile(args.input())

    /*
    parse text
    for example {"A B+"} ->{("A B",1)}
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
      1, 100000, true, 100, 0.5)

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
