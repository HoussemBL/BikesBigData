package solution

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import stream._
import db._
import java.io.FileInputStream
import java.util.Properties
import scala.io.Source
import Utils.Utils

object QueryBikes {
  def main(args: Array[String]): Unit = {

 val spark:SparkSession = Utils.getSpark()

    val timeTravelDF_1 = spark.read.format("delta")
      .option("versionAsOf", 0)
      .load("file:/home/houssem/delta-bikes/bikes")
    timeTravelDF_1.show(5, false)
    
    
    
    
    
   

  }
  

  
 
  
}
