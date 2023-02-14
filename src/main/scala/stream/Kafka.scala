package stream
import org.apache.spark.sql.types._
import java.util.Properties
import scala.io.Source

/****Interface of kafka producers/consumers****/
trait Kafka {
  def timewindow: Long
  def topic: String
}

//companion object
object Kafka {

  //schema used to read/write logs in kafka topics


      
      
//      final val schema = new StructType()
//      .add("results", ArrayType(new StructType()
//        .add("gender", StringType, true)
//        .add("name", ArrayType(new StructType()
//          .add("title", StringType)
//          .add("first", StringType)
//          .add("last", StringType)
//        ) )
//        .add("nat", StringType, true)
//      ))
          
      
     final val schema = new StructType()
        .add("gender", StringType, true)
        .add("name", ArrayType(new StructType()
          .add("title", StringType)
          .add("first", StringType)
          .add("last", StringType)
        ) )
        .add("nat", StringType, true)
      
  
  
  val schema2 = new StructType()
    .add("gender", StringType, true)
    .add("name", schema1, true)
    .add("nat", StringType, true)

  val schema1 = new StructType()
    .add("title", StringType, true)
    .add("first", StringType, true)
    .add("last", StringType, true)

  //specify batch interval as a string
  def convertTimeToString(wait_time: Long): String = {
    var processingTime = wait_time + " seconds"
    processingTime
  }

  //access the schema used for reading apache logs
  def getschema(): StructType = {
    Kafka.schema
  }

  //read properties of kafka specified in src.main.resources
  def readKafkaProperties(): Properties =
    {
      val url = getClass.getResource("/kafka.properties")
      val source = Source.fromURL(url)
      val Kafkaparameters = new Properties
      Kafkaparameters.load(source.bufferedReader())

      Kafkaparameters

    }
}

