package stream



 import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

/****KafkaProduce class provides the set of methods necessary to write in a kafka topic**/
case class KafkaProducer(topic: String, timewindow:Long) extends Kafka

//companion object
object KafkaProducer{
  
  //write in a topic the stream of logs
def writelogs_topic(topic:String, intervalBatch: Long,df: DataFrame)={  
  
val ds =  df.selectExpr("CAST(ip AS STRING) AS key", "to_json(struct(*)) AS value")
  val df_writing=ds.writeStream
   .format("kafka")
    .outputMode("append")
   .option("kafka.bootstrap.servers", "localhost:9092")
   .option("topic", topic)
   .option("checkpointLocation", "/tmp/checkpoints")
   .trigger(Trigger.ProcessingTime(intervalBatch))
   .start()
    
      
   df_writing.awaitTermination()  
}

//
////
//def getschema() :StructType= {
//  Kafka.schema
//  }

}