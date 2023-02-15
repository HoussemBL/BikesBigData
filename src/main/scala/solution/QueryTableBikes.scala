package solution

import Utils.Utils
import io.delta.tables._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object QueryTableBikes {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = Utils.getSpark()

    //spark.catalog.listTables().show(false)

    //retrieve history
    val deltaTable = DeltaTable.forName("bikes")
    val fullHistoryDF = deltaTable.history() // get the full history of the table
    fullHistoryDF.show(fullHistoryDF.count().toInt, false)

    //update data
    deltaTable.update( // predicate using Spark SQL functions and implicits
      col("id") === 1462668L,
      Map("frame_model" -> lit("click1")))

    val df_diff = spark.read.format("delta")
      .option("readChangeData", "true")
      .option("startingVersion", 0)
      .option("endingVersion", 1)
      .table("bikes")

    df_diff.printSchema()
    df_diff.show(false)
    print("end")





  }

}
