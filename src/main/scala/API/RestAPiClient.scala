package API

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.util.ByteString
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.{List, Properties}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


trait RestAPIconsumption {

  def callSearchBikeEndpoint()(implicit executionContext: ExecutionContext, system: ActorSystem): Unit

  protected def callEndpointGetBikeById(bike_id: String, url: String)(implicit executionContext: ExecutionContext, system: ActorSystem): Try[Int]


}

//class used to call to GET requests that should be depending on each other
//URL1: to search stolen bikes
//URL2: to get a specific BIKE info based on its id
//kafka_producer
class RestApiClient(url1: String, url2: String, kafka_properties: Properties)
  extends RestAPIconsumption {


  //Kafka_producer
  val kafka_producer = new KafkaProducer[String, String](kafka_properties)
  val kafka_topic = kafka_properties.getProperty("kafka_topic")

  //call first endpoint about searchingBike
  @throws(classOf[Exception])
 @throws(classOf[IllegalArgumentException])
 override def callSearchBikeEndpoint()(implicit executionContext: ExecutionContext, system: ActorSystem): Unit = {


    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url1))
    val gson = new Gson()

    val recoveredFuture: Future[HttpResponse] = responseFuture.recover {
      case _: Exception =>
        throw new java.lang.IllegalArgumentException("url1 is wrong")
    }

    recoveredFuture
      .onComplete {
        case Success(res) =>
          res.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
            val mapType = new TypeToken[java.util.HashMap[String, java.util.ArrayList[BikeInfo]]] {}.getType
            val bikeResult = Try(gson.fromJson(body.utf8String, mapType).asInstanceOf[java.util.Map[String, java.util.ArrayList[BikeInfo]]])
            bikeResult match {
              case Success(output) => {


                val list_bikes_id = output.get("bikes").asScala.map(bike => bike.id.toString)



                println(body.utf8String)
                for (bike_id <- list_bikes_id) {
                  callEndpointGetBikeById(bike_id, url2) match {
                    case Failure(_) => throw new java.lang.IllegalArgumentException("url2 is wrong")
                  }
                }
              }


              case Failure(_) =>
                //println(s"Error occurred: ${e.getMessage}")
                throw new java.lang.IllegalArgumentException("url2 is wrong")


            }

          }


        case Failure(_) => throw new java.lang.IllegalArgumentException("url1 is wrong") //sys.error("something went wrong")

      }


  }

  @throws(classOf[Exception])
  @throws(classOf[IllegalArgumentException])
  override def callEndpointGetBikeById(bike_id: String, url: String)(implicit executionContext: ExecutionContext,
                                                                     system: ActorSystem): Try[Int] = {
    //second end point

   var res:Try[Int]=Success(1);
    val url_bikeInfo = url + bike_id

    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url_bikeInfo))

    val recoveredFuture: Future[HttpResponse] = responseFuture.recover {
      case _: Exception =>
        //throw new java.lang.IllegalArgumentException("url2 is wrong")
        return Failure(new java.lang.IllegalArgumentException("url2 is wrong"))
    }

     recoveredFuture
      .onComplete {
        case Success(res) =>
          res.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
            val bike_id_info = body.utf8String
            println(bike_id_info)
            val record = new ProducerRecord[String, String](kafka_topic, bike_id_info)
            kafka_producer.send(record)
          }


        case Failure(_) =>

          //throw new java.lang.IllegalArgumentException("url2 is wrong")
         res= Failure(new java.lang.IllegalArgumentException("url2 is wrong"))

      }
      res
  }

}






