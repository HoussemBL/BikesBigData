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
  def storeData_mysql(intervalBatch: Long,df_read: StreamingQuery) = {
    while (df_read.isActive) {
      if (df_read.lastProgress != null) {
        val time_visit = df_read.lastProgress.timestamp
        val visit_num = df_read.lastProgress.sink.numOutputRows
        //DAO_visit.insert(time_visit, visit_num)
        println ( "rows number --> "+df_read.lastProgress.sink.numOutputRows)
        Thread.sleep(intervalBatch * 1000 - 1000)
      }
    }

  }
  
  

}