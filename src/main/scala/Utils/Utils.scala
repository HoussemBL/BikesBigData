package Utils



import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import stream._



object Utils{
  
  
     //get spark
  def getSpark():SparkSession={
    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExample")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .getOrCreate()
  spark    
  }
 
  
  //return a tuple containing 3 information
  // 1 path  from it we will read in real time logs
  // 2 the kafka topic 
  // 3 the time window of batch execution)
  def getKafkaParameters(): KafkaParameters={
    
    val   Kafka_parameters=Kafka.readKafkaProperties()    
val kafka_topic=Kafka_parameters.getProperty("kafka_topic")
val  timewindow=Kafka_parameters.getProperty("timewindow").toLong
val path_datasource=Kafka_parameters.getProperty("path_datasource")


 //println("path used to produce kafka --> "+ path_datasource)
    println("Name of the kafka topic --> "+ kafka_topic)
    println("Interval of batch in seconds --> "+  timewindow)   
     Thread.sleep(3000)

  val param= KafkaParameters(path_datasource,kafka_topic,timewindow)
  param
  }
  
}