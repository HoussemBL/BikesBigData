package stream


import Utils.Utils
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQuery


trait KafkaConsumerTrait {
  def readRawStream():DataFrame
  def persistStreamDF(df_bikes: DataFrame):StreamingQuery
  def convertStreamToDF(schemas: List[StructType], df_st: DataFrame): DataFrame
  protected def extractComponentsDimensionTable(schemas: List[StructType], df_source: DataFrame) :DataFrame
  protected def extractImagesDimensionTable(schemas: List[StructType], df_source: DataFrame):DataFrame

  protected def extractRecordStolenDimensionTable(schemas: List[StructType], df_source: DataFrame): DataFrame

  protected def print_console_StreamingDF(intervalBatchStr: String, df_out: DataFrame): StreamingQuery

  protected def save_delta(df_read: DataFrame, batchId: Long, path: String):Unit
}

case class KafkaConsumer(topic: String, timeWindowInSec: Long, kafka_Server:String) extends KafkaConsumerTrait
 {
   val spark = Utils.getSpark()


   //read initially the stream as it is
  override  def readRawStream():DataFrame = {

     spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", kafka_Server)
       .option("subscribe", topic)
       .option("startingOffsets", "earliest")
       .load()
   }



   //save all data that we read from the stream of bik
  override def persistStreamDF(df_bikes: DataFrame):StreamingQuery = {
    df_bikes.writeStream
      .trigger(Trigger.ProcessingTime(Kafka.convertTimeToString(timeWindowInSec)))
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        save_delta(batchDF
          , batchId, "bikes")

        val dfStolenRecord = extractRecordStolenDimensionTable(Kafka.schemas, batchDF)
        val dfComponents = extractComponentsDimensionTable(Kafka.schemas, batchDF)
        val dfImages = extractImagesDimensionTable(Kafka.schemas, batchDF)

        save_delta(dfStolenRecord, batchId, "stolenRecord")
        save_delta(dfComponents, batchId, "components")
        save_delta(dfImages, batchId, "images")
      }
      .start()
  }


  // transform the data consumed from a kafka topic to a dataframe  using the specified schema
  override def convertStreamToDF(schemas: List[StructType], df_st: DataFrame): DataFrame = {
    val bikesStreamDF = df_st.selectExpr("CAST(value AS STRING)")
    val bikesInfoDF = bikesStreamDF.select(from_json(col("value"), schemas(0)).as("data"))
      .select("data.*")

    val rawBikesDF1 = bikesInfoDF.select(from_json(col("bike"), schemas(1)).as("data"))
      .select("data.*")

    rawBikesDF1
  }


  override def extractImagesDimensionTable(schemas: List[StructType], df_source: DataFrame):DataFrame = {
    val bikesInfoImagesDF = df_source
      .select("id", "public_images")
      .withColumn("public_images2", explode(col("public_images")))
      .select(col("id"), from_json(col("public_images2"), schemas(3)).as("public_images"))
    bikesInfoImagesDF
  }

  override def extractComponentsDimensionTable(schemas: List[StructType], df_source: DataFrame):DataFrame = {
    val bikesInfoComponentsDF = df_source
      .select("id", "components")
      .withColumn("components2", explode(col("components")))
      .select(col("id"), from_json(col("components2"), schemas(3)).as("components"))
    bikesInfoComponentsDF
  }

   override def extractRecordStolenDimensionTable(schemas: List[StructType], df_source: DataFrame): DataFrame = {
     val bikesRecordStolenDF = df_source
       .select(from_json(col("stolen_record"), schemas(2)).as("stolen_record"))
       .select("stolen_Record.date_stolen", "stolen_Record.location", "stolen_Record.latitude",
         "stolen_Record.longitude", "stolen_Record.theft_Description", "stolen_Record.locking_description",
         "stolen_Record.lock_defeat_description", "stolen_Record.police_report_number", "stolen_Record.police_report_department",
         "stolen_Record.created_at", "stolen_Record.create_open311", "stolen_Record.id")
     bikesRecordStolenDF
   }

  //query the data stream available as dataframe ( function used for debugging)
   override def print_console_StreamingDF(intervalBatchStr: String, df_out: DataFrame): StreamingQuery = {
    val df = df_out.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(intervalBatchStr))
      .start()
    df
  }

  //store data in delta
  protected override def save_delta(df_read: DataFrame, batchId: Long, path: String):Unit = {
    df_read
      .withColumn("loaded_at", current_timestamp())
      .withColumn("batch_id", lit(batchId))
      .write
      .format("delta")
      .mode("append")
      .option("overwriteSchema", "true")
      .option("delta.enableChangeDataFeed", "true")
      //.save(path)
      .saveAsTable(path)
  }


}