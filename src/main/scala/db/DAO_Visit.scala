package db
import java.sql._
//import com.typesafe.config._
import java.util.Properties
import scala.io.Source


//class used to insert data in mysql DB
case class DAO_visit(timestamp: String, count: Long)


//companion object
object DAO_visit {

  val insertSql = """
                    |insert into visits_stats(timestamp,number_visit)
                    |values (?,?)
""".stripMargin

  //read properties of mysql specified in src.main.resources
  def readMYSQLProperties(): Properties =
  {
    val url = getClass.getResource("/mysql.properties")
    val source = Source.fromURL(url)
    val mysqlparameters = new Properties
    mysqlparameters.load(source.bufferedReader())

    mysqlparameters

  }




  def getURL() :String= readMYSQLProperties().getProperty("db_url")
  def getTable() :String= readMYSQLProperties().getProperty("table")
  def getUser() :String= readMYSQLProperties().getProperty("mysql_user")
  def getPass() :String= readMYSQLProperties().getProperty("mysql_pass")
  def getDriver() :String= readMYSQLProperties().getProperty("jdbc_driver")








}
