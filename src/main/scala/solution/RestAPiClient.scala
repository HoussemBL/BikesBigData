package solution



import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


import java.util.Properties
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

import java.util
import java.util.Map
import java.util.List
import scala.collection.immutable.TreeMap




//import scala.collection.mutable.ListBuffer

case class BikeInfo(date_stolen:Long, description:String, frame_colors:List[String], frame_model:String,
                    id:Long, is_stock_img :Boolean, large_img: String, location_found:Boolean,
                    manufacturer_name:String, external_id:Long, registry_name: String,
                    registry_url:String, serial: String, status:String, stolen:Boolean,
                    stolen_coordinates :List[Float], stolen_location:String, thumb:String,
                    title:String, url: String, year: Int
                   )


case class BikeArray(bikes: java.util.List[BikeInfo])

object RestApiClient {



  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher




    val url = "https://bikeindex.org/api/v3/search?page=1&per_page=1&location=address&stolenness=all"

    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url))


    system.scheduler.schedule(0.seconds, 15.seconds) {
      callSearchBikeEndpoint(responseFuture)
    }



  }

  def sendToKafka(data: String,topic:String): Unit = {
    val brokers = "localhost:9092"
    val properties = new Properties()
    properties.put("bootstrap.servers", brokers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    val record = new ProducerRecord[String, String](topic, data)
    producer.send(record)
    producer.close()
  }



//call first endpoint about searchingBike
  def callSearchBikeEndpoint(responseFuture :Future[HttpResponse])(implicit executionContext: ExecutionContext,
  system : ActorSystem, materializer: ActorMaterializer) = {
    //implicit val system = ActorSystem()
  //  implicit val materializer = ActorMaterializer()
   // implicit val executionContext = system.dispatcher
    val gson = new Gson()
    responseFuture
      .onComplete {
        case Success(res) =>
          res.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
            val bikes = body.utf8String


            val mapType = new TypeToken[java.util.HashMap[String, java.util.ArrayList[BikeInfo]]] {}.getType
            val person = Try(gson.fromJson(body.utf8String, mapType).asInstanceOf[java.util.Map[String, java.util.ArrayList[BikeInfo]]])
            person match {
              case Success(output) => {
                val bike_id = output.get("bikes").get(0).id.toString
                //println(body.utf8String)
                sendToKafka(bikes, "searchbikes")

                println(body.utf8String)
                callEnpointGetBikeById(bike_id)
              }
              case Failure(e) => {
                println(s"Error occurred: ${e.getMessage}")
              }
            }

          }


        case Failure(_) => sys.error("something went wrong")
      }
  }
  def callEnpointGetBikeById(bike_id: String)(implicit executionContext: ExecutionContext,
                                              system: ActorSystem, materializer: ActorMaterializer) = {
    //second end point


    val url_bikeInfo = "https://bikeindex.org/api/v3/bikes/" + bike_id

    val responseFuture2: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url_bikeInfo))
    responseFuture2
      .onComplete {
        case Success(res) =>
          res.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
            val bike_id_info = body.utf8String
            println(bike_id_info)
            sendToKafka(bike_id_info, "infobikes")

          }
        case Failure(_) => sys.error("something went wrong when searching the bike " + bike_id)
      }
  }
}




