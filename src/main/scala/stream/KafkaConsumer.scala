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
object KafkaConsumer{
  
 // transform the data consumed from a kafka topic to a dataframe  using the specified schema
  def convertStreamToDF(schemas: List[StructType],df_st: DataFrame): DataFrame = {

    
    val bikesInfoRawDF = df_st.selectExpr("CAST(value AS STRING)")
    val bikesInfoDF = bikesInfoRawDF.select(from_json(col("value"), schemas(0)).as("data"))
    .select("data.*")
    bikesInfoDF.printSchema()
    val bikesInfoDF1 =  bikesInfoDF .select(from_json(col("bike"),schemas(1)).as("data"))
      .select("data.*")
    bikesInfoDF1.printSchema()


     //new part
     val bikesInfoDF2 =  bikesInfoDF1.select(col("date_stolen"),col("description"),col("frame_colors"),
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
        col("extra_registration_number"),col("additional_registration"),
        from_json(col("Stolen_record"),schemas(2)).as("stolen_record"),
        from_json(col("public_images"),schemas(3)).as("public_images"),
        from_json(col("components"),schemas(4)).as("components")
        //explode(col("components").as("components"))
      )

    //val tt =bikesInfoDF.schema.map(x=>"col("+x.name+")").mkString(",")
    bikesInfoDF2.printSchema()

    return bikesInfoDF1
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


  //store number of visits into mysql
  def save_cassandra(df_read: DataFrame) = {
    val mm: Map[String, String] = Map[String, String]("table" -> DAO_Cassandra.getTable(), "keyspace" -> DAO_Cassandra.getKeySpace())
    // while (df_read.isActive) {
    df_read.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(mm)
      .save()
  }

  //store statistics into mysql
  def save_mysql(df_read: DataFrame, batchId: Long) = {

    val df_agg = df_read.groupBy("country").agg(count("id").as("total_sum"))
      .withColumn("processed_at", current_timestamp())
      .withColumn("batch_id", lit(batchId))
      .select("batch_id", "country", "processed_at", "total_sum")

    df_agg.write.format("jdbc")
      .option("url", DAO_visit.getURL())
      .option("dbtable", DAO_visit.getTable())
      .option("user", DAO_visit.getUser())
      .option("password", DAO_visit.getPass())
      .mode("append")
      .save()


  }

  //load function (used later for testing)
  def loadDF(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read.option("header", true).option("inferSchema", true).csv(path)
    df
  }

}