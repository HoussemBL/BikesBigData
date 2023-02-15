package stream

import db._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.kafka010.KafkaUtils


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.functions._


case class KafkaConsumer(topic: String, timewindow: Long) extends Kafka


//companion object  
object KafkaConsumer {

  // transform the data consumed from a kafka topic to a dataframe  using the specified schema
  def convertStreamToDF(schemas: List[StructType], df_st: DataFrame): DataFrame = {


    val bikesStreamDF = df_st.selectExpr("CAST(value AS STRING)")
    val bikesInfoDF = bikesStreamDF.select(from_json(col("value"), schemas(0)).as("data"))
      .select("data.*")
    //bikesInfoDF.printSchema()
    val rawBikesDF1 = bikesInfoDF.select(from_json(col("bike"), schemas(1)).as("data"))
      .select("data.*")

    rawBikesDF1.printSchema()


    //apply this part if you want to drop some columns
    /*   val bikesInfoDF2 =  rawBikesDF1.select(col("date_stolen"),col("description"),col("frame_colors"),
          col("frame_model"),col("id"),col("is_stock_img"),
          col("large_img"),col("location_found"),col("manufacturer_name"),
          col("external_id"),col("registry_url"),col("serial"),
          col("status"),col("stolen"),col("stolen_coordinates"),
          col("stolen_location"),col("thumb"),col("title"),
          col("url"),col("year"),col("registration_created_at"),
          col("registration_updated_at"),col("api_url"),
          col("manufacturer_id"),col("paint_description"),col("name"),col("frame_size"),col("rear_tire_narrow"),col("front_tire_narrow"),
          col("type_of_cycle"),col("test_bike"),
          col("rear_wheel_size_iso_bsd"), col("front_wheel_size_iso_bsd"),
          col("handlebar_type_slug"), col("frame_material_slug"),
          col("front_gear_type_slug"),col("rear_gear_type_slug"),
          col("extra_registration_number"),col("additional_registration")
         ,col("stolen_record"),col("public_images"),col("components")
        )





      bikesInfoDF2.printSchema()


      //return List(bikesInfoDF2,bikesInfoImagesDF,bikesInfoComponentsDF,bikesRecordStolenDF)
      return bikesInfoDF2*/
    return rawBikesDF1
  }


  def extractImagesDimensionTable(schemas: List[StructType], df_source: DataFrame) = {

    val bikesInfoImagesDF = df_source
      .select("id", "public_images")
      .withColumn("public_images2", explode(col("public_images")))
      .select(col("id"), from_json(col("public_images2"), schemas(3)).as("public_images"))
    bikesInfoImagesDF
  }

  def extractComponentsDimensionTable(schemas: List[StructType], df_source: DataFrame) = {
    val bikesInfoComponentsDF = df_source
      .select("id", "components")
      .withColumn("components2", explode(col("components")))
      .select(col("id"), from_json(col("components2"), schemas(3)).as("components"))
    bikesInfoComponentsDF
  }

  def extractRecordStolenDimensionTable(schemas: List[StructType], df_source: DataFrame) = {
    val bikesRecordStolenDF = df_source
      .select(from_json(col("stolen_record"), schemas(2)).as("stolen_record"))
      .select("stolen_Record.date_stolen", "stolen_Record.location", "stolen_Record.latitude",
        "stolen_Record.longitude", "stolen_Record.theft_Description", "stolen_Record.locking_description",
        "stolen_Record.lock_defeat_description", "stolen_Record.police_report_number", "stolen_Record.police_report_department",
        "stolen_Record.created_at", "stolen_Record.create_open311", "stolen_Record.id")
    bikesRecordStolenDF
  }

  //query the data stream available as dataframe
  def print_console_StreamingDF(intervalBatchStr: String, df_out: DataFrame): StreamingQuery = {
    val df = df_out.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(intervalBatchStr))
      .start()
    df
  }

  //store data in delta
  def save_delta(df_read: DataFrame, batchId: Long, path: String) = {

    df_read
      .withColumn("loaded_at", current_timestamp())
      .withColumn("batch_id", lit(batchId))
      .write
      .format("delta")
      .mode(/*"overwrite"*/"append")
      .option("overwriteSchema", "true")
      .option("delta.enableChangeDataFeed", "true")
      //.save(path)
      .saveAsTable(path)
      //.insertInto(path)
  }



  //load function (used later for testing)
  def loadDF(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read.option("header", true).option("inferSchema", true).csv(path)
    df
  }

}