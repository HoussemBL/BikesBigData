package Spark

import Utils.Utils
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter}
import stream.{Kafka, KafkaConsumer}

class KafkaConsumerTestSuite extends AnyFunSuite with BeforeAndAfter {


  //read it in generic way like string
  def createSyntheticStreamData(): DataFrame = {
    val schema = new StructType()
      .add("value", StringType)
    val df = spark.readStream
      .schema(schema)
      .json("file:/home/houssem/scala-workspace/BikesBigData/src/main/resources/data/")

    df
  }

  //read it in specified way
  def createSyntheticBikeData(): DataFrame = {

    val df = spark.read
      .schema(Kafka.schemaBike)
      .json("file:/home/houssem/scala-workspace/BikesBigData/src/main/resources/data/")

    df
  }


  var spark: SparkSession = _

  before {
    spark = Utils.getSpark()
  }

  after {
    spark.stop()
  }


  /*
  * {"bike":{"date_stolen":1676530800,"description":"E-Bike","frame_colors":["Silver, gray or bare metal"],"frame_model":"","id":1453230,"is_stock_img":false,"large_img":null,"location_found":null,"manufacturer_name":"SparkX","external_id":null,"registry_name":null,"registry_url":null,"serial":"19060305","status":"stolen","stolen":true,"stolen_coordinates":[48.43,-123.37],"stolen_location":"Victoria, CA","thumb":null,"title":"SparkX","url":"https://bikeindex.org/bikes/1453230","year":null,"registration_created_at":1673237612,"registration_updated_at":1676531969,"api_url":"https://bikeindex.org/api/v1/bikes/1453230","manufacturer_id":100,"paint_description":null,"name":"SparkX","frame_size":"xl","rear_tire_narrow":true,"front_tire_narrow":null,"type_of_cycle":"Bike","test_bike":false,"rear_wheel_size_iso_bsd":null,"front_wheel_size_iso_bsd":null,"handlebar_type_slug":null,"frame_material_slug":null,"front_gear_type_slug":null,"rear_gear_type_slug":null,"extra_registration_number":"","additional_registration":"","stolen_record":{"date_stolen":1676530800,"location":"Victoria, CA","latitude":48.43,"longitude":-123.37,"theft_description":"","locking_description":"","lock_defeat_description":"","police_report_number":"","police_report_department":"Victoria","created_at":1676531775,"create_open311":false,"id":139620},"public_images":[],"components":[]}}
  {"bike":{"date_stolen":1676528158,"description":null,"frame_colors":["Black"],"frame_model":"Shine","id":1462980,"is_stock_img":false,"large_img":null,"location_found":null,"manufacturer_name":"Honda","external_id":null,"registry_name":null,"registry_url":null,"serial":"WB24Q8231","status":"stolen","stolen":true,"stolen_coordinates":[22.64,88.41],"stolen_location":"Kolkata, 700065, IN","thumb":null,"title":"2010 Honda Shine","url":"https://bikeindex.org/bikes/1462980","year":2010,"registration_created_at":1676528554,"registration_updated_at":1676528564,"api_url":"https://bikeindex.org/api/v1/bikes/1462980","manufacturer_id":100,"paint_description":null,"name":null,"frame_size":null,"rear_tire_narrow":true,"front_tire_narrow":null,"type_of_cycle":"Bike","test_bike":false,"rear_wheel_size_iso_bsd":null,"front_wheel_size_iso_bsd":null,"handlebar_type_slug":null,"frame_material_slug":null,"front_gear_type_slug":null,"rear_gear_type_slug":null,"extra_registration_number":null,"additional_registration":null,"stolen_record":{"date_stolen":1676528158,"location":"Kolkata, 700065, IN","latitude":22.64,"longitude":88.41,"theft_description":null,"locking_description":null,"lock_defeat_description":null,"police_report_number":null,"police_report_department":null,"created_at":1676528554,"create_open311":false,"id":139617},"public_images":[],"components":[]}}
  {"bike":{"date_stolen":1676530620,"description":null,"frame_colors":["Black","Red"],"frame_model":null,"id":1462984,"is_stock_img":false,"large_img":"https://files.bikeindex.org/uploads/Pu/674716/large_IMG_1670.PNG","location_found":null,"manufacturer_name":"Giant","external_id":null,"registry_name":null,"registry_url":null,"serial":"WGI K1FK14218 R 0312","status":"stolen","stolen":true,"stolen_coordinates":[38.54,-121.74],"stolen_location":"Davis, CA 95616, US","thumb":"https://files.bikeindex.org/uploads/Pu/674716/small_IMG_1670.PNG","title":"Giant","url":"https://bikeindex.org/bikes/1462984","year":null,"registration_created_at":1676530711,"registration_updated_at":1676530918,"api_url":"https://bikeindex.org/api/v1/bikes/1462984","manufacturer_id":153,"paint_description":null,"name":null,"frame_size":null,"rear_tire_narrow":true,"front_tire_narrow":null,"type_of_cycle":"Bike","test_bike":false,"rear_wheel_size_iso_bsd":null,"front_wheel_size_iso_bsd":null,"handlebar_type_slug":"flat","frame_material_slug":null,"front_gear_type_slug":"3","rear_gear_type_slug":"7","extra_registration_number":null,"additional_registration":null,"stolen_record":{"date_stolen":1676530620,"location":"Davis, CA 95616, US","latitude":38.54,"longitude":-121.74,"theft_description":null,"locking_description":null,"lock_defeat_description":null,"police_report_number":null,"police_report_department":null,"created_at":1676530711,"create_open311":false,"id":139619},"public_images":[{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674716/IMG_1670.PNG","large":"https://files.bikeindex.org/uploads/Pu/674716/large_IMG_1670.PNG","medium":"https://files.bikeindex.org/uploads/Pu/674716/medium_IMG_1670.PNG","thumb":"https://files.bikeindex.org/uploads/Pu/674716/small_IMG_1670.PNG","id":674716},{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674717/IMG_1671.PNG","large":"https://files.bikeindex.org/uploads/Pu/674717/large_IMG_1671.PNG","medium":"https://files.bikeindex.org/uploads/Pu/674717/medium_IMG_1671.PNG","thumb":"https://files.bikeindex.org/uploads/Pu/674717/small_IMG_1671.PNG","id":674717},{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674718/IMG_1673.jpg","large":"https://files.bikeindex.org/uploads/Pu/674718/large_IMG_1673.jpg","medium":"https://files.bikeindex.org/uploads/Pu/674718/medium_IMG_1673.jpg","thumb":"https://files.bikeindex.org/uploads/Pu/674718/small_IMG_1673.jpg","id":674718},{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674719/IMG_1677.PNG","large":"https://files.bikeindex.org/uploads/Pu/674719/large_IMG_1677.PNG","medium":"https://files.bikeindex.org/uploads/Pu/674719/medium_IMG_1677.PNG","thumb":"https://files.bikeindex.org/uploads/Pu/674719/small_IMG_1677.PNG","id":674719},{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674715/IMG_1674.jpg","large":"https://files.bikeindex.org/uploads/Pu/674715/large_IMG_1674.jpg","medium":"https://files.bikeindex.org/uploads/Pu/674715/medium_IMG_1674.jpg","thumb":"https://files.bikeindex.org/uploads/Pu/674715/small_IMG_1674.jpg","id":674715}],"components":[{"id":1146067,"description":"Front and Back fenders for wheels","serial_number":"","component_type":"fender","component_group":"Additional parts","rear":null,"front":null,"manufacturer_name":null,"model_name":"","year":null}]}}
  {"bike":{"date_stolen":1676530620,"description":null,"frame_colors":["Black","Red"],"frame_model":null,"id":1462984,"is_stock_img":false,"large_img":"https://files.bikeindex.org/uploads/Pu/674716/large_IMG_1670.PNG","location_found":null,"manufacturer_name":"Giant","external_id":null,"registry_name":null,"registry_url":null,"serial":"WGI K1FK14218 R 0312","status":"stolen","stolen":true,"stolen_coordinates":[38.54,-121.74],"stolen_location":"Davis, CA 95616, US","thumb":"https://files.bikeindex.org/uploads/Pu/674716/small_IMG_1670.PNG","title":"Giant","url":"https://bikeindex.org/bikes/1462984","year":null,"registration_created_at":1676530711,"registration_updated_at":1676530918,"api_url":"https://bikeindex.org/api/v1/bikes/1462984","manufacturer_id":153,"paint_description":null,"name":null,"frame_size":null,"rear_tire_narrow":true,"front_tire_narrow":null,"type_of_cycle":"Bike","test_bike":false,"rear_wheel_size_iso_bsd":null,"front_wheel_size_iso_bsd":null,"handlebar_type_slug":"flat","frame_material_slug":null,"front_gear_type_slug":"3","rear_gear_type_slug":"7","extra_registration_number":null,"additional_registration":null,"stolen_record":{"date_stolen":1676530620,"location":"Davis, CA 95616, US","latitude":38.54,"longitude":-121.74,"theft_description":null,"locking_description":null,"lock_defeat_description":null,"police_report_number":null,"police_report_department":null,"created_at":1676530711,"create_open311":false,"id":139619},"public_images":[{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674716/IMG_1670.PNG","large":"https://files.bikeindex.org/uploads/Pu/674716/large_IMG_1670.PNG","medium":"https://files.bikeindex.org/uploads/Pu/674716/medium_IMG_1670.PNG","thumb":"https://files.bikeindex.org/uploads/Pu/674716/small_IMG_1670.PNG","id":674716},{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674717/IMG_1671.PNG","large":"https://files.bikeindex.org/uploads/Pu/674717/large_IMG_1671.PNG","medium":"https://files.bikeindex.org/uploads/Pu/674717/medium_IMG_1671.PNG","thumb":"https://files.bikeindex.org/uploads/Pu/674717/small_IMG_1671.PNG","id":674717},{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674718/IMG_1673.jpg","large":"https://files.bikeindex.org/uploads/Pu/674718/large_IMG_1673.jpg","medium":"https://files.bikeindex.org/uploads/Pu/674718/medium_IMG_1673.jpg","thumb":"https://files.bikeindex.org/uploads/Pu/674718/small_IMG_1673.jpg","id":674718},{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674719/IMG_1677.PNG","large":"https://files.bikeindex.org/uploads/Pu/674719/large_IMG_1677.PNG","medium":"https://files.bikeindex.org/uploads/Pu/674719/medium_IMG_1677.PNG","thumb":"https://files.bikeindex.org/uploads/Pu/674719/small_IMG_1677.PNG","id":674719},{"name":"Giant Black and Red","full":"https://files.bikeindex.org/uploads/Pu/674715/IMG_1674.jpg","large":"https://files.bikeindex.org/uploads/Pu/674715/large_IMG_1674.jpg","medium":"https://files.bikeindex.org/uploads/Pu/674715/medium_IMG_1674.jpg","thumb":"https://files.bikeindex.org/uploads/Pu/674715/small_IMG_1674.jpg","id":674715}],"components":[{"id":1146067,"description":"Front and Back fenders for wheels","serial_number":"","component_type":"fender","component_group":"Additional parts","rear":null,"front":null,"manufacturer_name":null,"model_name":"","year":null}]}}
  {"bike":{"date_stolen":1676530800,"description":"E-Bike","frame_colors":["Silver, gray or bare metal"],"frame_model":"","id":1453230,"is_stock_img":false,"large_img":null,"location_found":null,"manufacturer_name":"SparkX","external_id":null,"registry_name":null,"registry_url":null,"serial":"19060305","status":"stolen","stolen":true,"stolen_coordinates":[48.43,-123.37],"stolen_location":"Victoria, CA","thumb":null,"title":"SparkX","url":"https://bikeindex.org/bikes/1453230","year":null,"registration_created_at":1673237612,"registration_updated_at":1676531969,"api_url":"https://bikeindex.org/api/v1/bikes/1453230","manufacturer_id":100,"paint_description":null,"name":"SparkX","frame_size":"xl","rear_tire_narrow":true,"front_tire_narrow":null,"type_of_cycle":"Bike","test_bike":false,"rear_wheel_size_iso_bsd":null,"front_wheel_size_iso_bsd":null,"handlebar_type_slug":null,"frame_material_slug":null,"front_gear_type_slug":null,"rear_gear_type_slug":null,"extra_registration_number":"","additional_registration":"","stolen_record":{"date_stolen":1676530800,"location":"Victoria, CA","latitude":48.43,"longitude":-123.37,"theft_description":"","locking_description":"","lock_defeat_description":"","police_report_number":"","police_report_department":"Victoria","created_at":1676531775,"create_open311":false,"id":139620},"public_images":[],"components":[]}}
  {"bike":{"date_stolen":1676528158,"description":null,"frame_colors":["Black"],"frame_model":"Shine","id":1462980,"is_stock_img":false,"large_img":null,"location_found":null,"manufacturer_name":"Honda","external_id":null,"registry_name":null,"registry_url":null,"serial":"WB24Q8231","status":"stolen","stolen":true,"stolen_coordinates":[22.64,88.41],"stolen_location":"Kolkata, 700065, IN","thumb":null,"title":"2010 Honda Shine","url":"https://bikeindex.org/bikes/1462980","year":2010,"registration_created_at":1676528554,"registration_updated_at":1676528564,"api_url":"https://bikeindex.org/api/v1/bikes/1462980","manufacturer_id":100,"paint_description":null,"name":null,"frame_size":null,"rear_tire_narrow":true,"front_tire_narrow":null,"type_of_cycle":"Bike","test_bike":false,"rear_wheel_size_iso_bsd":null,"front_wheel_size_iso_bsd":null,"handlebar_type_slug":null,"frame_material_slug":null,"front_gear_type_slug":null,"rear_gear_type_slug":null,"extra_registration_number":null,"additional_registration":null,"stolen_record":{"date_stolen":1676528158,"location":"Kolkata, 700065, IN","latitude":22.64,"longitude":88.41,"theft_description":null,"locking_description":null,"lock_defeat_description":null,"police_report_number":null,"police_report_department":null,"created_at":1676528554,"create_open311":false,"id":139617},"public_images":[],"components":[]}}
*/
  test("test convert stream to dataframe") {
    val df = createSyntheticStreamData()
    val actual = KafkaConsumer.convertStreamToDF(Kafka.schemas, df)

    actual.printSchema()

    //check the structure of dataframe is correct
    assert(actual.schema.fields.length == 42)
  }


  test("test extraction of stolen record") {
    val df = createSyntheticBikeData()
    val actual = KafkaConsumer.extractRecordStolenDimensionTable(Kafka.schemas,df)

    actual.printSchema()

    //check the structure of dataframe is correct
    assert(actual.schema.fields.length == 12)

    assert(actual.count() == 3)
  }

  /* test("word count sql") {
     import spark.implicits._
     val input = Seq("hello world", "hello spark", "world spark")
     val inputDF = input.toDF("text")

     val expected = Seq(("hello", 2), ("world", 2), ("spark", 2)).toDF("word", "count")
     val actual = inputDF
       .selectExpr("explode(split(text, ' ')) as word")
       .groupBy("word")
       .count()

     assertDataFrameEquals(expected, actual)
   }*/

}
