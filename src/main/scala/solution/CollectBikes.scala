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

    val spark = Utils.getSpark()
    import spark.implicits._
    val kafkaprameters = Utils.getKafkaParameters()


    //consuming Kafka topic
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", /*kafkaprameters.url*/ "localhost:9092")
      .option("subscribe", /*kafkaprameters.topic*/ "infobikes")
      .option("startingOffsets", "earliest") // From starting
      .load()


    val df_bikes = KafkaConsumer.convertStreamToDF(Kafka.schemas, df)
    //just use it for logging
    // val df_out = KafkaConsumer.convertStreamToDF(Kafka.schemas,df)(0)
    //val df_read = KafkaConsumer.print_console_StreamingDF(Kafka.convertTimeToString(kafkaprameters.timewindow),df_out)


    //write in delta
    val df_read = df_bikes.writeStream
      .trigger(Trigger.ProcessingTime(Kafka.convertTimeToString(kafkaprameters.timewindow)))
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        KafkaConsumer.save_delta(batchDF
          /*.withColumn("url",
          concat(col("url"),lit(("_Houssem"))))*/
          /*.drop("stolen_record","public_images","components")*/
            , batchId, /*"file:/home/houssem/delta-bikes/bikes"*/"bikes")

        val dfStolenRecord = KafkaConsumer.extractRecordStolenDimensionTable(Kafka.schemas, batchDF)
        val dfComponents = KafkaConsumer.extractComponentsDimensionTable(Kafka.schemas, batchDF)
        val dfImages = KafkaConsumer.extractImagesDimensionTable(Kafka.schemas, batchDF)

        KafkaConsumer.save_delta(dfStolenRecord, batchId,"stolenRecord" /*"file:/home/houssem/delta-bikes/stolen-record"*/)
        KafkaConsumer.save_delta(dfComponents, batchId, "components" /*"file:/home/houssem/delta-bikes/components"*/)
        KafkaConsumer.save_delta(dfImages, batchId, "images"/*"file:/home/houssem/delta-bikes/images"*/)
      }
      .start()



    df_read.awaitTermination()
  }


}

