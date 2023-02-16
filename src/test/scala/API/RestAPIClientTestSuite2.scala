package API

import akka.actor.ActorSystem
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import stream.Kafka

import scala.concurrent.{Await, Future}

class RestAPIClientTestSuite2 extends AnyFunSuite {
  implicit val system = ActorSystem()
  implicit val executionContext = system.dispatcher

  test("An empty List should have size 0") {
    assert(List.empty.size == 0)
  }

  //test empty url
  test("An empty url") {
    val properties = Kafka.readKafkaProperties()
    // val url1 = "x"
    val url1 = "" //"https://bikeindex.org/api/v3/search?page=1&per_page=3&location=address&stolenness=stolen"
    val url2 = ""
    val restAPIClient = new RestApiClient(url1, url2, properties)


    assertThrows[akka.http.scaladsl.model.IllegalUriException] {
      restAPIClient.callSearchBikeEndpoint()
    }
  }

    test("An empty url2") {
      val properties = Kafka.readKafkaProperties()
      // val url1 = "x"
      val url11 = "https://bikeindex.org/api/v3/search?page=1&per_page=3&location=address&stolenness=stolen"
      val url22 = ""
      val restAPIClient1 = new RestApiClient(url11, url22, properties)

      restAPIClient1.callSearchBikeEndpoint()
      assertThrows[java.lang.IllegalArgumentException] {
        //throw new java.lang.IllegalArgumentException("url2 is wrong")
        restAPIClient1.callSearchBikeEndpoint()
      }

    }




  //test wrong url


  //test wrong kafka params


}
