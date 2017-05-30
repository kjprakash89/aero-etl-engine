import net.liftweb.json._
import net.liftweb.json.DefaultFormats

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

import EtlFunc._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, DataFrame }
import org.apache.spark.sql.Column
import org.apache.log4j.{ Level, Logger }
import scala.io.Source
import aero.core.parser.EtlFunc._

object JobConfigParser extends App {

  val resultMap = scala.collection.mutable.Map.empty[String, DataFrame]


  implicit def list2Tuple4(in: List[Any]): scala.Tuple4[Any, String, Any, Any] = {
    (in(0), in(1).toString, in(2), if (in.size < 4) "" else in(3))
  }
  val myJson = parse(scala.io.Source.fromFile("""C:\tmp\test1.json""").mkString)
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("etl")
    .config("spark.driver.allowMultipleContexts", "true").config("spark.ui.port", "44040")
    .getOrCreate()

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  println(s"__job has been started__")

  myJson.children.map(x => x.values).foreach { conf =>
    list2Tuple4(conf.asInstanceOf[List[Any]]) match {
      case (in, "read", out, option) => {
        val df = readDataFrame(spark, option.asInstanceOf[Map[String, String]])
        
        println(s"__read dataframe  $out and ouput stored into map, and dataframe size"+ df.count)
        resultMap+=(out.toString()->df)
        println("numbers of dataframes in result map"+ resultMap.size)
      }
      case (in, "write", out, option) => println(in)
      case (in, "filter", out, option) => {
        val df = filterDataFrame(resultMap.get(in.toString).get, option.asInstanceOf[Map[String, String]])
        for(i<-0 to df.size-1)
        resultMap+=(out.asInstanceOf[List[String]](i)->df(i))
      }
      case x => println("not matched " + x)
    }
  }
  println("numbers of dataframes in result map"+ resultMap.size)
  resultMap.foreach(_._2.show())

  println(s"__job has been completed__")
  spark.stop

}
