package solution


import org.apache.spark.sql.SparkSession
import io.delta.tables._
import Utils.Utils

object QueryBikes {
  def main(args: Array[String]): Unit = {

 val spark:SparkSession = Utils.getSpark()




   //retrieve history
   val deltaTable = DeltaTable.forPath(spark, "file:/home/houssem/delta-bikes/bikes")

    val fullHistoryDF = deltaTable.history() // get the full history of the table

    fullHistoryDF.show(fullHistoryDF.count().toInt,false)


    val lastOperationDF = deltaTable.history(1)
    lastOperationDF.show(fullHistoryDF.count().toInt,false)

    //check content of first dataframe
    val timeTravelDF_0 = spark.read.format("delta")
      .option("versionAsOf", 0)
      .load("file:/home/houssem/delta-bikes/bikes")
    timeTravelDF_0.show(5, false)


    //check content of last dataframe
    val timeTravelDF_1 = spark.read.format("delta")
      .option("versionAsOf", 4)
      .load("file:/home/houssem/delta-bikes/bikes")
    timeTravelDF_1.show(5, false)
   
  print("end")


  
 //query delta
val df=spark.read.format("delta")
  .load("file:/home/houssem/delta-bikes/bikes")

  val numberofStolenBikes= df
    .count()
    println ("numberofStolenBikes " + numberofStolenBikes)

    df.groupBy("type_of_cycle").count().show()
/*
* +-------------+-----+
|type_of_cycle|count|
+-------------+-----+
|         Bike|   28|
| E-skateboard|    1|
|     Tricycle|    1|
|    Recumbent|    2|
+-------------+-----+
* */

   df.groupBy("year").count().show()
  }

}
