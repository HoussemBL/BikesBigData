package solution

import API.RestApiClient
import akka.actor.ActorSystem
import stream.Kafka

object CallAPI {
  private val properties = Kafka.readKafkaProperties()
  private val url1 = "sssss"//"https://bikeindex.org/api/v3/search?page=1&per_page=30&location=address&stolenness=stolen"
  private val url2 = "https://bikeindex.org/api/v3/bikes/"


  private val restAPIClient = new RestApiClient(url1, url2, properties)

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val executionContext = system.dispatcher


    while (true) {
      restAPIClient.callAPI()
      Thread.sleep(1 * 60 * 1000) // sleep for 5 minutes

    }


  }

}
