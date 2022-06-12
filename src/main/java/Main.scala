package main.java
//import org.apache.spark.rdd.RDD
//wordCounts.take(5)
import org.apache.spark.{SparkConf,SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/Users/zhushengping/wikiOfSpark.txt")
    val wordRDD = input.flatMap(line => line.split(" ")).filter(word => !word.equals(""))
    val wordCounts= wordRDD.map(word => (word, 1)).reduceByKey(_+_)
    wordCounts.foreach(println)
    sc.stop()
  }
}