package analytics

import Utils.Utils
import io.delta.tables._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import java.time

import java.io.{File, FileOutputStream, PrintStream}

object QueryHistoryBikes {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = Utils.getSpark()

    // Redirecting the standard output to a file
    val consoleOutputFile = new File("/home/houssem/scala-workspace/BikesBigData/src/main/resources/output/file.txt")//+time.LocalDateTime.now().toString+".txt")
    val fileOutputStream = new FileOutputStream(consoleOutputFile)
    val printStream = new PrintStream(fileOutputStream)
    Console.withOut(printStream) {

      //retrieve history
      val deltaTable = DeltaTable.forPath(spark, "file:/home/houssem/scala-workspace/BikesBigData/spark-warehouse/bikes")

      //history dataframe
      println("history dataframe")
      val fullHistoryDF = deltaTable.history() // get the full history of the table
      fullHistoryDF.show(false)


      //check version 0
      //check content of first dataframe
      println("content of dataframe v0")
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
      println("history dataframe")
      fullHistoryDF.show(false)


      //val lastOperationDF = deltaTable.history(1)
      //lastOperationDF.show(false)


      //check content of last dataframe
      println("content of dataframe v1")
      val timeTravelDF_1 = spark.read.format("delta")
        .option("versionAsOf", 1)
        .load("file:/home/houssem/scala-workspace/BikesBigData/spark-warehouse/bikes")
      timeTravelDF_1.show(5, false)




      //difference between two versions
      println("compare content dataframe v1 to v0")
      timeTravelDF_1.except(timeTravelDF_0).show(false)

      println("compare content dataframe v0 to v1")
      timeTravelDF_0.except(timeTravelDF_1).show(false)

     /* val df_diff = spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .option("endingVersion", 1)
        .load("file:/home/houssem/scala-workspace/BikesBigData/spark-warehouse/bikes")

      df_diff.printSchema()
      df_diff.show(false)
      */

    }




  }

}
