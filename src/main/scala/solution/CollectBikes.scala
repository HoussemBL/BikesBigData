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

import java.util.Properties
import scala.io.Source
import Utils.Utils


object CollectBikes {
  def main(args: Array[String]): Unit = {

val   spark=Utils.getSpark()
import spark.implicits._
 val kafkaprameters= Utils.getKafkaParameters()


    //consuming Kafka topic
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", /*kafkaprameters.url*/"localhost:9092")
      .option("subscribe", /*kafkaprameters.topic*/"infobikes")
      .option("startingOffsets", "earliest") // From starting
      .load()


    val df_out = KafkaConsumer.convertStreamToDF(Kafka.schemas,df)



    val df_read = KafkaConsumer.print_console_StreamingDF(Kafka.convertTimeToString(kafkaprameters.timewindow),df_out)
    println("ss")
   // KafkaConsumer.storeData_mysql(kafkaprameters.timewindow,df_read)

    df_read.awaitTermination()

  }
  
  


}
