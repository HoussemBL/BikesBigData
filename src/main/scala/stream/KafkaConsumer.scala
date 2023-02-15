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
     .select(from_json(col("bike"),schemas(1)).as("data"))
      .select("data.*")



 //     .select(explode(col("bike")).as("col"))
   //   .select("col.date_stolen","col.description")
    bikesInfoDF.printSchema()

    return bikesInfoDF
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