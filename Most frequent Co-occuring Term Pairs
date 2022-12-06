import org.apache.spark.SparkContext

import org.apache.spark.rdd._

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf

object Problem1 {

  def main(args: Array[String]) {

    val number = args(0).toInt

    val stopwords = args(1)

    val inputFile = args(2)

    val outputFolder = args(3)

    val conf = new SparkConf().setAppName("LetterCount").setMaster("local")

    val sc = new SparkContext(conf)

    

    //Map out every word which satisfies the project constraints

    val textFile = sc.textFile(inputFile).map(_.split(",")).map(x => x(1).toLowerCase)

    

    //broadcast every stopword

    val SetStopWords = sc.broadcast(sc.textFile(stopwords).collect.toSet)

    

    //Make each line into a list of words and filter out stopwords

    val text = textFile.map(_.split(" ").filter(x => x.charAt(0) <='z' && x.charAt(0) >='a').toList.filter(!SetStopWords.value.contains(_)).sorted)

    

    //Use combinations function to create word pairs and map them with value 1. Then sum all values with same key.

    val result = text.flatMap(_.combinations(2)).map((_,1)).reduceByKey(_ + _)

    

    //sorting the by value then key to get project format

    val answer = result.sortBy(_._1(1)).sortBy(_._1(0)).sortBy(_._2,false)

    

    //printing output into textfile

    val answer1 = answer.map(x => s"${x._1(0)},${x._1(1)}\t${x._2}").take(number)

    sc.parallelize(answer1,1).saveAsTextFile(outputFolder)

    sc.stop()

  }

}
