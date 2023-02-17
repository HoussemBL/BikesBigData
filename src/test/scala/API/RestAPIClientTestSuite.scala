package API

import akka.actor.ActorSystem
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import stream.Kafka

import scala.concurrent.{Await, Future}
import scala.util.Try

class RestAPIClientTestSuite extends AnyFunSuite {
  implicit val system = ActorSystem()
  implicit val executionContext = system.dispatcher


  //test empty url
  test("An empty url for the two calls") {
    val properties = Kafka.readKafkaProperties()
    val url1 = ""
    val url2 = ""
    val restAPIClient = new RestApiClient(url1, url2, properties)


    assertThrows[akka.http.scaladsl.model.IllegalUriException] {
      restAPIClient.callAPI()
    }
  }







  test("an empty url1") {
    val properties = Kafka.readKafkaProperties()
    val url1 = ""
    val url2 = "https://bikeindex.org/api/v3/bikes/"
    val restAPIClient = new RestApiClient(url1, url2, properties)


    assertThrows[akka.http.scaladsl.model.IllegalUriException] {
      restAPIClient.callAPI()
    }

  }


  test("not empty url1 , but wrong") {
    val properties = Kafka.readKafkaProperties()
    val url1 = "sasasasa"
    val url2 = "https://bikeindex.org/api/v3/bikes/"
    val restAPIClient = new RestApiClient(url1, url2, properties)



    val futureResult: Future[Try[Int]] = restAPIClient.callAPI()

    val result: Try[Int] = Await.result(futureResult, 5.seconds)

    assert(result.isFailure)
  }


  test("an empty url2") {
    val properties = Kafka.readKafkaProperties()
    val url11 = "https://bikeindex.org/api/v3/search?page=1&per_page=3&location=address&stolenness=stolen"
    val url22 = ""
    val restAPIClient1 = new RestApiClient(url11, url22, properties)
    val futureResult: Future[Try[Int]] = restAPIClient1.callAPI()

    val result: Try[Int] = Await.result(futureResult, 5.seconds)

    assert(result.isFailure)
  }

  test("non empty wrong url2") {
    val properties = Kafka.readKafkaProperties()
    // val url1 = "x"
    val url11 = "https://bikeindex.org/api/v3/search?page=1&per_page=3&location=address&stolenness=stolen"
    val url22 = "sssssss"
    val restAPIClient1 = new RestApiClient(url11, url22, properties)
    val futureResult: Future[Try[Int]] = restAPIClient1.callAPI()

    val result: Try[Int] = Await.result(futureResult, 5.seconds)

    assert(result.isFailure)
  }


 /* test("wrong kafka info") {
    val properties = Kafka.readKafkaProperties()
    val url11 = "https://bikeindex.org/api/v3/search?page=1&per_page=3&location=address&stolenness=stolen"
    val url22 = "https://bikeindex.org/api/v3/bikes/"
    properties.setProperty("kafka_topic","wrong")
    val restAPIClient1 = new RestApiClient(url11, url22,properties )
    val futureResult: Future[Try[Int]] = restAPIClient1.generalCall()

    val result: Try[Int] = Await.result(futureResult, 5.seconds)

    assert(result.isFailure)
  }*/



  test("correct urls") {
    val properties = Kafka.readKafkaProperties()
    val url11 = "https://bikeindex.org/api/v3/search?page=1&per_page=3&location=address&stolenness=stolen"
    val url22 = "https://bikeindex.org/api/v3/bikes/"
    val restAPIClient1 = new RestApiClient(url11, url22, properties)
    val futureResult: Future[Try[Int]] = restAPIClient1.callAPI()

    val result: Try[Int] = Await.result(futureResult, 5.seconds)

    assert(result.isSuccess)
  }

}
