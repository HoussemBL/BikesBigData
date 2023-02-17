package solution


import stream._
import Utils.Utils


object CollectBikes {
  def main(args: Array[String]): Unit = {

    val kafkaParameters = Utils.getKafkaParameters()
    val kafkaConsumer = KafkaConsumer(kafkaParameters.topic, kafkaParameters.timewindow, kafkaParameters.url)


    //consuming Kafka topic
    val df = kafkaConsumer.readRawStream()


    val df_bikes = kafkaConsumer.convertStreamToDF(Kafka.schemas, df)

    //just use it for logging
    // val df_out = KafkaConsumer.convertStreamToDF(Kafka.schemas,df)(0)
    //val df_read = KafkaConsumer.print_console_StreamingDF(Kafka.convertTimeToString(kafkaprameters.timewindow),df_out)


    //write in delta
    val df_read = kafkaConsumer.persistStreamDF(df_bikes)


    df_read.awaitTermination()
  }


}

