package analytics

import Utils.Utils
import io.delta.tables._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object QueryHistoryBikes {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = Utils.getSpark()


    //retrieve history
    val deltaTable = DeltaTable.forPath(spark, "file:/home/houssem/scala-workspace/BikesBigData/spark-warehouse/bikes")

    //history dataframe
    val fullHistoryDF = deltaTable.history() // get the full history of the table
    fullHistoryDF.show(false)

    //check version 0
    //check content of first dataframe
    val timeTravelDF_0 = spark.read.format("delta")
      .option("versionAsOf", 0)
      .load("file:/home/houssem/scala-workspace/BikesBigData/spark-warehouse/bikes")
    timeTravelDF_0.show(5, false)

    val bike_id_to_update = timeTravelDF_0.head().getAs[Long](4)


    //update data
    deltaTable.update( // predicate using Spark SQL functions and implicits
      col("id") === bike_id_to_update,
      Map("frame_model" -> lit("click2")))

    //check the history of transformations
    fullHistoryDF.show(fullHistoryDF.count().toInt, false)


    val lastOperationDF = deltaTable.history(1)
    lastOperationDF.show(fullHistoryDF.count().toInt, false)


    //check content of last dataframe
    val timeTravelDF_1 = spark.read.format("delta")
      .option("versionAsOf", 1)
      .load("file:/home/houssem/scala-workspace/BikesBigData/spark-warehouse/bikes")
    timeTravelDF_1.show(5, false)




    //difference between two versions
    timeTravelDF_1.except(timeTravelDF_0).show(false)

    val df_diff = spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 0)
      .option("endingVersion", 1)
      //.table("myDeltaTable")
      .load("file:/home/houssem/scala-workspace/BikesBigData/spark-warehouse/bikes")

    df_diff.printSchema()
    df_diff.show(false)
    print("end")


  }

}
