package db
import java.sql._
//import com.typesafe.config._
import java.util.Properties
import scala.io.Source


//class used to insert data in mysql DB
case class DAO_Cassandra(timestamp: String, count: Long)


//companion object
object DAO_Cassandra {



  //read properties of mysql specified in src.main.resources
  def read_Cassandra_properties(): Properties =
  {
    val url = getClass.getResource("/cassandra.properties")
    val source = Source.fromURL(url)
    val cassandra_parameters= new Properties
    cassandra_parameters.load(source.bufferedReader())

    cassandra_parameters

  }

  def getTable() :String= read_Cassandra_properties().getProperty("table")

  def getKeySpace() :String= read_Cassandra_properties().getProperty("keyspace")

  def getPort() :String= read_Cassandra_properties().getProperty("port")
}

