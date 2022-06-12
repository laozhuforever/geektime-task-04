package main.java

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

//实现思路
//1. 读取目录下的文件，并生成列表
//2. 遍历文件，并读取文件类容成成Rdd，结构为（文件名，单词）并将多个Rdd拼接成1个Rdd
//3. 构建词频（（文件名，单词），词频）
//4. 调整输出格式,将（文件名，单词），词频）==》 （单词，（文件名，词频）） ==》 （单词，（文件名，词频））汇总

object GeekInvertedIndex {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("invertedIndex").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val input = "/Users/zhushengping/iindex2"
    //遍历文件
    val fs = FileSystem.get(sc.hadoopConfiguration)
    //递归遍历目录下所有文件,取出内容生成RDD,格式为(文件名，单词)
    val fileList = fs.listFiles( new Path(input),true)
       // rdd声明变量为 var,可变
    var unionrdd = sc.emptyRDD[(String,String)]
    while (fileList.hasNext) {
      val abs_path = new Path(fileList.next().getPath.toString)
      val file_name = abs_path.getName
      val rdd1 = sc.textFile(abs_path.toString).flatMap(_.split(" ").map((file_name, _)))
        //将遍历的多个rdd拼接成1个Rdd
      unionrdd = unionrdd.union(rdd1)
    }
    //统计词频，格式为（（文件名，单词），词频）
    val rdd2 = unionrdd.map(word => {(word, 1)}).reduceByKey(_ + _)
    //调整格式为(单词，(文件名，词频)) =>汇总
    val frdd1 = rdd2.map(word=>{(word._1._2,String.format("(%s,%s)",word._1._1,word._2.toString))})
    val frdd2 = frdd1.reduceByKey(_ +","+ _)
    val frdd3 = frdd2.map(word =>String.format("\"%s\",{%s}",word._1,word._2))
    //unionrdd.foreach(println)
    //println("\n")
    //rdd2.foreach(println)
    //println("\n")
    //frdd1.foreach(println)
    //println("\n")
    //frdd2.foreach(println)
    //println("\n")
    frdd3.foreach(println)
    sc.stop()
  }
}