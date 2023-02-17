package API

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.util.ByteString
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}


trait RestAPIconsumption {

  protected def callSearchBikeEndpoint()(implicit executionContext: ExecutionContext, system: ActorSystem): Future[Option[List[String]]]

  protected def callEndpointGetBikeById(url: String, list_bikes_id: List[String])(implicit executionContext: ExecutionContext, system: ActorSystem): Future[List[Try[Int]]]

  def callAPI()(implicit executionContext: ExecutionContext, system: ActorSystem): Future[Try[Int]]
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


  override def callAPI()(implicit executionContext: ExecutionContext, system: ActorSystem): Future[Try[Int]] = {

    val op = for {
      listBikes <- callSearchBikeEndpoint()
      callResult <- callEndpointGetBikeById(url2, listBikes.getOrElse(List[String]()))
    }
    yield callResult


    op.map { listCallResult =>
      if (listCallResult.length > 0) {
        val hasFailure: Boolean = listCallResult.exists {
          case Failure(_) => true
          case _ => false
        }

        if (hasFailure) Failure(new java.lang.IllegalArgumentException("url2 is wrong"))
        else Success(1)
      }
      else {
        println("empty result from url1")
        Failure(new java.lang.IllegalArgumentException("url1 is wrong"))}
    }.recover {
      case e: Exception => Failure(new java.lang.IllegalArgumentException("url1 is wrong"))
    }

  }


  //call first endpoint about searchingBike
  @throws(classOf[Exception])
  @throws(classOf[IllegalArgumentException])
  override def callSearchBikeEndpoint()(implicit executionContext: ExecutionContext, system: ActorSystem): Future[Option[List[String]]] = {


    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url1))
    val gson = new Gson()

    val result: Future[Option[List[String]]] = responseFuture.flatMap { httpResponse =>
      httpResponse.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
        val mapType = new TypeToken[java.util.HashMap[String, java.util.ArrayList[BikeInfo]]] {}.getType
        val bikeResult = gson.fromJson(body.utf8String, mapType).asInstanceOf[java.util.Map[String, java.util.ArrayList[BikeInfo]]]
        val list_bikes_id = bikeResult.get("bikes").asScala.map(bike => bike.id.toString).toList

        Option(list_bikes_id)
      }
    }
      .recover {
        case e : Exception => None
      }
    result
  }


  //second end point
  @throws(classOf[Exception])
  @throws(classOf[IllegalArgumentException])
  override def callEndpointGetBikeById(url: String, list_bikes_id: List[String])(implicit executionContext: ExecutionContext,
                                                                                 system: ActorSystem): Future[List[Try[Int]]] = {


    val res = Future.sequence {
      list_bikes_id.map { bike_id =>

        val url_bikeInfo = url + bike_id
        val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url_bikeInfo))

        responseFuture
          .map { httpResponse =>
            httpResponse.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map { body =>
              val bike_id_info = body.utf8String
              println(bike_id_info)
              val record = new ProducerRecord[String, String](kafka_topic, bike_id_info)
              kafka_producer.send(record)
            }
            Success(1)
          }.recover { case e: Exception =>
          Failure(new java.lang.IllegalArgumentException("url2 is wrong"))
          //throw new java.lang.IllegalArgumentException("url2 is wrong")
        }
      }
    }

    res

  }

}






