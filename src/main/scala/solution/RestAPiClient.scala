package solution





import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties


object RestApiClient {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val url = "https://bikeindex.org/api/v3/search?page=1&per_page=1&location=address&stolenness=all"

    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url))

   /* responseFuture
      .onComplete {
        case Success(res) =>
          res.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
            sendToKafka(body.utf8String)
            //println(body.utf8String)
          }

        case Failure(_) => sys.error("something went wrong")
      }*/

   // system.scheduler.schedule(0.seconds, 5.seconds) {
    system.scheduler.schedule(0.minutes, 5.seconds) {
      responseFuture
        .onComplete {
          case Success(res) =>
            res.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
              val bikes=body.utf8String
              sendToKafka(bikes,"bikes")
              //println(body.utf8String)
              body
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




