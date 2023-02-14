package solution



import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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

case class BikeInfo(date_stolen:Double, description:String, frame_colors:List[String], frame_model:String,
                    id:Double, is_stock_img :Boolean, large_img: String, location_found:Boolean,
                    manufacturer_name:String, external_id:Double, registry_name: String,
                    registry_url:String, serial: String, status:String, stolen:Boolean,
                    stolen_coordinates :List[Float], stolen_location:String, thumb:String,
                    title:String, url: String, year: Int
                   )

case class BikeInfo2(date_stolen:Double
                   )

/*{"bikes":[{"date_stolen":1676358000,"description":null,"frame_colors":["Yellow or Gold","Green","Black"],
  "frame_model":"G8rMXt7RduiUWmvV1d","id":1462524,"is_stock_img":false,
  "large_img":null,"location_found":null,"manufacturer_name":"39eVYP87lP4Vl",
  "external_id":null,"registry_name":null,"registry_url":null,"serial":"LiR4FJXMD1D5BMUm",
  "status":"stolen","stolen":true,"stolen_coordinates":[-30.56,22.94],
  "stolen_location":"W Zz Wa Nqq Z Hy Fu Sqn5m, NC EASTSIDE50044@GMAIL.COM, ZA",
  "thumb":null,"title":"1918 39eVYP87lP4Vl G8rMXt7RduiUWmvV1d cargo bike (front storage)",
  "url":"https://bikeindex.org/bikes/1462524","year":1918}]}*/


case class BikeArray(bikes: java.util.List[BikeInfo])

object RestApiClient {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher




    val url = "https://bikeindex.org/api/v3/search?page=1&per_page=1&location=address&stolenness=all"

    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url))
    val gson = new Gson()

    system.scheduler.schedule(0.minutes, 5.minutes) {
      responseFuture
        .onComplete {
          case Success(res) =>
            res.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
              val bikes = body.utf8String


              val mapType = new TypeToken[java.util.HashMap[String, java.util.ArrayList[BikeInfo]]] {}.getType
              val person = Try(gson.fromJson(body.utf8String,mapType).asInstanceOf[java.util.Map[String, java.util.ArrayList[BikeInfo]]])
               person match {
                case Success(output) => {
                  println(body.utf8String)
                  sendToKafka(bikes, "bikes")
                }
                case Failure(e) => {
                  println(s"Error occurred: ${e.getMessage}")
                }
              }
              println(body.utf8String)
            }


          case Failure(_) => sys.error("something went wrong")
        }

    }

  }
  def sendToKafka(data: String,topic:String): Unit = {
    //val topic = "bikes"
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


}




