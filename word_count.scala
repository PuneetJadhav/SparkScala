package org.apache.spark.project1.copy
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object wordcount {
  
  def main(args:Array[String]){
    val sc= new SparkContext("local[*]","wordcount")
    val data=sc.textFile("C:/College/Hadoop/SparkScala/book.txt")
    val data_to_string=data.map(x => x.toString())
    val words=data_to_string.flatMap(x =>x.split("\\s+"))
    val word_tup=words.map(x=>(x,1))
    val results=word_tup.reduceByKey((x,y) => x+y)
    val sorted_results=results.sortBy(_._2)
    sorted_results.collect().foreach(println)
  
    
    
  }
}