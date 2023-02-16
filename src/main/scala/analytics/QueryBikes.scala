package analytics

import Utils.Utils
import io.delta.tables._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object QueryBikes {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = Utils.getSpark()

    //query delta
    val df = spark.read.format("delta")
      .load("file:/home/houssem/delta-bikes/bikes")

    val numberofStolenBikes = df
      .count()
    println("numberofStolenBikes " + numberofStolenBikes)

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
