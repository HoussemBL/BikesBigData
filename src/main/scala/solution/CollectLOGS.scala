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

object CollectLOGS {
  def main(args: Array[String]): Unit = {

 val spark:SparkSession = Utils.getSpark()
 
 val kafkaprameters= Utils.getKafkaParameters()
 
 


    //reading stream of logs
    val dfCSV = spark.readStream.option("sep", " ")
      .option("header", "false")
      .schema(Kafka.getschema())
       .csv(kafkaprameters.path_datasource)
   
     
    
    //writing in kafka topic
     KafkaProducer.writelogs_topic(kafkaprameters.topic, kafkaprameters.timewindow,dfCSV)
    
    
    
    
    
    
   

  }
  

  
 
  
}
