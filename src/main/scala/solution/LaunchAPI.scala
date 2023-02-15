package solution

import akka.actor.ActorSystem
import org.apache.kafka.clients.producer.KafkaProducer
import stream.Kafka

object LaunchAPI {
  val properties = Kafka.readKafkaProperties()
  val url1 = "https://bikeindex.org/api/v3/search?page=1&per_page=3&location=address&stolenness=stolen"
  val  url2 = "https://bikeindex.org/api/v3/bikes/"

  val restAPIClient = new RestApiClient(url1,url2,properties)

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val executionContext = system.dispatcher


    while (true) {
      restAPIClient.callSearchBikeEndpoint()
      Thread.sleep(1 * 60 * 1000) // sleep for 5 minutes

    }


  }

}
