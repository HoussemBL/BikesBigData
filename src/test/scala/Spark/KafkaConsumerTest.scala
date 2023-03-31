package Spark

import Utils.Utils
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter}
import stream.{Kafka, KafkaConsumer}
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class KafkaConsumerTest extends AnyFunSuite with BeforeAndAfter {
//path of bikes json persisted locally
  val pathJsonFiles = getClass.getResource("/data").getPath


  //read data as astream
  def createSyntheticStreamData(): DataFrame = {
    val schema = new StructType()
      .add("value", StringType)
    val df = spark.readStream
      .schema(schema)
      .json(pathJsonFiles)

    df
  }

  //read data properly from the source
  def createSyntheticBikeData(): DataFrame = {

    val df = spark.read
      .schema(Kafka.schemaBike)
      .json(pathJsonFiles)
    df
  }


  var spark: SparkSession = _
  var kafkaConsumer: KafkaConsumer = _
  before {
    spark = Utils.getSpark()
    kafkaConsumer = KafkaConsumer("topic", 60, "kafka_Server")
  }

  after {
    spark.stop()
  }



  test("test convert stream to dataframe") {
    val df = createSyntheticStreamData()
    val actual = kafkaConsumer.convertStreamToDF(Kafka.schemas, df)

    //check the structure of dataframe is correct
    assert(actual.schema.fields.length == 42)
  }


  test("test extraction of stolen record") {
    val df = createSyntheticBikeData()
    val actual = kafkaConsumer.extractRecordStolenDimensionTable(Kafka.schemas,df)

    //check the structure of dataframe is correct
    assert(actual.schema.fields.length == 12)

    //check that dataframe contains as expected 3 records
    assert(actual.count() == 3)
  }


}
