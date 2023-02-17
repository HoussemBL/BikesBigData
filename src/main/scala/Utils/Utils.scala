package Utils



import org.apache.spark.sql.SparkSession
import stream._


object Utils {


  //get spark
  def getSpark(): SparkSession = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExample")
      .enableHiveSupport()
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark
  }


  //return a tuple containing 2 information
  // 2 the kafka topic 
  // 3 the time window of batch execution)
  def getKafkaParameters(): KafkaParameters = {

    val Kafka_parameters = Kafka.readKafkaProperties()
    val kafka_topic = Kafka_parameters.getProperty("kafka_topic")
    val timewindow = Kafka_parameters.getProperty("timewindow").toLong
    val url = Kafka_parameters.getProperty("bootstrap.servers")


    println("Name of the kafka topic --> " + kafka_topic)
    println("Interval of batch in seconds --> " + timewindow)
    Thread.sleep(3000)

    val param = KafkaParameters(kafka_topic, timewindow, url)
    param
  }

}