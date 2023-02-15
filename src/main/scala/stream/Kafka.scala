package stream
import org.apache.spark.sql.types.{StructType, _}

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


  final val schemaBasic = new StructType()
    .add("bike", StringType, true)

  final val schemaBike = new StructType()
      .add("date_stolen", LongType,nullable = true)
      .add("description", StringType,nullable = true)
      .add("frame_colors", ArrayType(StringType),nullable = true)
      .add("frame_model", StringType,nullable = true)
      .add("id", LongType,nullable = true)
      .add("is_stock_img", BooleanType,nullable = true)
      .add("large_img", StringType,nullable = true)
      .add("location_found", BooleanType,nullable = true)
      .add("manufacturer_name", StringType,nullable = true)
      .add("external_id", LongType,nullable = true)
      .add("registry_url", StringType,nullable = true)
      .add("serial", StringType,nullable = true)
      .add("status", StringType,nullable = true)
      .add("stolen", BooleanType,nullable = true)
      .add("stolen_coordinates", ArrayType(FloatType),nullable = true)
      .add("stolen_location", StringType,nullable = true)
      .add("thumb", StringType,nullable = true)
      .add("title", StringType,nullable = true)
      .add("url", StringType,nullable = true)
      .add("year", IntegerType,nullable = true)
      .add("registration_created_at", LongType,nullable = true)
      .add("registration_updated_at", LongType,nullable = true)
      .add("api_url", StringType,nullable = true)
      .add("manufacturer_id", LongType,nullable = true)
      .add("paint_description", StringType,nullable = true)
      .add("name", StringType,nullable = true)
      .add("frame_size", StringType,nullable = true)
      .add("rear_tire_narrow", BooleanType,nullable = true)
      .add("front_tire_narrow", BooleanType,nullable = true)
      .add("type_of_cycle", StringType,nullable = true)
      .add("test_bike", BooleanType,nullable = true)
      .add("rear_wheel_size_iso_bsd", StringType,nullable = true)
      .add("front_wheel_size_iso_bsd", StringType,nullable = true)
      .add("handlebar_type_slug", StringType,nullable = true)
      .add("frame_material_slug", StringType,nullable = true)
      .add("front_gear_type_slug", StringType,nullable = true)
      .add("rear_gear_type_slug", StringType,nullable = true)
      .add("extra_registration_number", StringType,nullable = true)
      .add("additional_registration", StringType,nullable = true)
      .add("stolen_record", StringType,nullable = true)
      .add("public_images", ArrayType(StringType),nullable = true)
      .add("components", ArrayType(StringType),nullable = true)

  final val schemaStolenRecord = new StructType ()
    .add("date_stolen", LongType,nullable = true)
    .add("location", StringType,nullable = true)
    .add("latitude", DoubleType,nullable = true)
    .add("longitude", DoubleType,nullable = true)
    .add("theft_Description", StringType,nullable = true)
    .add("locking_description", StringType,nullable = true)
    .add("lock_defeat_description", StringType,nullable = true)
    .add("police_report_number", StringType,nullable = true)
    .add("police_report_department", StringType,nullable = true)
    .add("created_at", LongType,nullable = true)
    .add("create_open311", BooleanType,nullable = true)
    .add("id", LongType,nullable = true)



  val schemaImage= new StructType()
    .add("name", StringType,nullable = true)
    .add("full", StringType,nullable = true)
    .add("large", StringType,nullable = true)
    .add("medium", StringType,nullable = true)
    .add("thumb", StringType,nullable = true)
    .add("id", LongType,nullable = true)


  val schemaComponents = new StructType()
    .add("id", LongType,nullable = true)
    .add("description", StringType,nullable = true)
    .add("serial_number", LongType,nullable = true)
    .add("component_type", StringType,nullable = true)
    .add("component_group", StringType,nullable = true)
    .add("rear", StringType,nullable = true)
    .add("front", StringType,nullable = true)
    .add("manufacturer_name", StringType,nullable = true)
    .add("model_name", StringType,nullable = true)
    .add("year", IntegerType,nullable = true)


  final val schemas:List[StructType]= List[StructType](schemaBasic,schemaBike,schemaStolenRecord,schemaImage,schemaComponents)

  final val schemaBikeInfo = new StructType()
        .add("bike", ArrayType(new StructType()
          .add("date_stolen", LongType)
          .add("description", StringType)
          .add("frame_colors", ArrayType(StringType))
          .add("frame_model", StringType)
          .add("id", LongType)
          .add("is_stock_img", BooleanType)
          .add("large_img", StringType)
          .add("location_found", BooleanType)
          .add("manufacturer_name",StringType)
          .add("external_id",LongType)
          .add("registry_url",StringType)
          .add("serial",StringType)
          .add("status",StringType)
          .add("stolen",BooleanType)
          .add("stolen_coordinates",ArrayType(FloatType))
          .add("stolen_location",StringType)
          .add("thumb",StringType)
          .add("title",StringType)
          .add("url",StringType)
          .add("year",IntegerType)
          .add("registration_created_at",LongType)
          .add("registration_updated_at",LongType)
          .add("api_url",StringType)
          .add("manufacturer_id",LongType)
          .add("paint_description",StringType)
          .add("name",StringType)
          .add("frame_size",StringType)
          .add("rear_tire_narrow",BooleanType)
          .add("front_tire_narrow",BooleanType)
          .add("type_of_cycle",StringType)
          .add("test_bike",BooleanType)
          .add("rear_wheel_size_iso_bsd",StringType)
          .add("front_wheel_size_iso_bsd",StringType)
          .add("handlebar_type_slug",StringType)
          .add("frame_material_slug",StringType)
          .add("front_gear_type_slug",StringType)
          .add("rear_gear_type_slug",StringType)
          .add("extra_registration_number",StringType)
          .add("additional_registration",StringType)
          .add("stolen_record",new StructType()
            .add("date_stolen",LongType)
            .add("location",StringType)
            .add("latitude", DoubleType)
            .add("longitude", DoubleType)
            .add("theft_Description",StringType)
            .add("locking_description",StringType)
            .add("lock_defeat_description",StringType)
            .add("police_report_number",StringType)
            .add("police_report_department",StringType)
            .add("created_at",LongType)
            .add("create_open311",BooleanType)
            .add("id",LongType)
          )
          .add("public_images",ArrayType(new StructType()
            .add("name",StringType)
            .add("full",StringType)
            .add("large",StringType)
            .add("meduim",StringType)
            .add("thumb",StringType)
            .add("id",LongType)
          ))
          .add("components",ArrayType(StringType))
        ) )

      
  
  


  //specify batch interval as a string
  def convertTimeToString(wait_time: Long): String = {
    var processingTime = wait_time + " seconds"
    processingTime
  }

  //access the schema used for reading apache logs
  /*def getschema(): StructType = {
    Kafka.schemaBikeInfo
  }*/

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

