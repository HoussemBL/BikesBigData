package solution


import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import stream._

import Utils.Utils


object CollectBikes {
  def main(args: Array[String]): Unit = {

    val spark = Utils.getSpark()
    import spark.implicits._
    val kafkaprameters = Utils.getKafkaParameters()


    //consuming Kafka topic
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaprameters.url/*"localhost:9092"*/ )
      .option("subscribe", kafkaprameters.topic)
      .option("startingOffsets", "earliest") // From starting
      .load()


    val df_bikes = KafkaConsumer.convertStreamToDF(Kafka.schemas, df)
    //just use it for logging
    // val df_out = KafkaConsumer.convertStreamToDF(Kafka.schemas,df)(0)
    //val df_read = KafkaConsumer.print_console_StreamingDF(Kafka.convertTimeToString(kafkaprameters.timewindow),df_out)


    //write in delta
    val df_read = KafkaConsumer.persistStreamDF(df_bikes,kafkaprameters.timewindow)


    df_read.awaitTermination()
  }


}

