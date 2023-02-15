package solution


import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.util.ByteString
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import solution.LaunchAPI.properties
import stream.Kafka

import java.util.{List, Properties}
import scala.collection.JavaConverters._

case class BikeInfo(date_stolen: Long, description: String, frame_colors: List[String], frame_model: String,
                    id: Long, is_stock_img: Boolean, large_img: String, location_found: Boolean,
                    manufacturer_name: String, external_id: Long, registry_name: String,
                    registry_url: String, serial: String, status: String, stolen: Boolean,
                    stolen_coordinates: List[Float], stolen_location: String, thumb: String,
                    title: String, url: String, year: Int
                   )


case class BikeArray(bikes: java.util.List[BikeInfo])


//class used to call to GET requests that should be depending on each other
//URL1: to search stolen bikes
//URL2: to get a specific BIKE info based on its id
//kafka_producer
class RestApiClient(url1: String, url2: String, kafka_properties: Properties) {

  //Kafka_producer
  val kafka_producer = new KafkaProducer[String, String](kafka_properties)
  val kafka_topic = kafka_properties.getProperty("kafka_topic")

  //call first endpoint about searchingBike
  def callSearchBikeEndpoint()(implicit executionContext: ExecutionContext, system: ActorSystem) = {

    val url = url1 //"https://bikeindex.org/api/v3/search?page=1&per_page=3&location=address&stolenness=stolen"
    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url))
    val gson = new Gson()
    responseFuture
      .onComplete {
        case Success(res) =>
          res.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
            val mapType = new TypeToken[java.util.HashMap[String, java.util.ArrayList[BikeInfo]]] {}.getType
            val person = Try(gson.fromJson(body.utf8String, mapType).asInstanceOf[java.util.Map[String, java.util.ArrayList[BikeInfo]]])
            person match {
              case Success(output) => {


                val list_bikes_id = output.get("bikes").asScala.map(bike => bike.id.toString)


                //val record = new ProducerRecord[String, String]("searchbikes", bikes)
                //producer.send(record)

                println(body.utf8String)
                for (bike_id <- list_bikes_id) {
                  callEnpointGetBikeById(bike_id, url2)
                }
              }
              case Failure(e) => {
                println(s"Error occurred: ${e.getMessage}")
              }
            }

          }


        case Failure(_) => sys.error("something went wrong")
      }
  }

  //get Bike info given a specified bike_id
  // url: url of the endpoint
  def callEnpointGetBikeById(bike_id: String, url: String)(implicit executionContext: ExecutionContext,
                                                           system: ActorSystem) = {
    //second end point


    val url_bikeInfo = /*"https://bikeindex.org/api/v3/bikes/"*/ url + bike_id

    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url_bikeInfo))
    responseFuture
      .onComplete {
        case Success(res) =>
          res.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
            val bike_id_info = body.utf8String
            println(bike_id_info)


            val record = new ProducerRecord[String, String](kafka_topic, bike_id_info)
            kafka_producer.send(record)

          }
        case Failure(_) => sys.error("something went wrong when searching the bike " + bike_id)
      }
  }

}






