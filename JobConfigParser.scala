package aero.core
package parser

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import EtlFunc.filterDataFrame
import EtlFunc.joinDataFrame
import EtlFunc.readDataFrame
import EtlFunc.writeDataFrame
import net.liftweb.json.JValue
import net.liftweb.json.parse

object JobConfigParser extends App {

  lazy val spark = SparkSession
    .builder()
    .master("local")
    .appName("etl")
    .config("spark.driver.allowMultipleContexts", "true").config("spark.ui.port", "44040")
    .getOrCreate()
   spark.sparkContext.setLogLevel("ERROR")
  
  lazy val resultMap = scala.collection.mutable.Map.empty[String, DataFrame]
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  
  etlMain
  
  println("printing the results from result map of size " + resultMap.size)
  resultMap.foreach(x => {
    println(x._1)
    x._2.show()
  })
  spark.stop

  def etlMain() = {
    val myJson = parse(scala.io.Source.fromFile("""C:\tmp\test1.json""").mkString)
    println(s"__job has been started__")
    myJson.children.foreach(switchCase(_))
    println(s"__job has been completed__")
  }

  def switchCase(confTuple: scala.Tuple4[Any, String, Any, Any]): Unit = {
    confTuple match {
      case (in, "read", out, option) => {
        val df = readDataFrame(spark, option.asInstanceOf[Map[String, String]])
        println(s"__read dataframe  $out and ouput stored into map, and dataframe size" + df.count)
        resultMap += (out.toString() -> df)
        println("numbers of dataframes in result map" + resultMap.size)
      }
      case (in, "write", out, option) => {
        writeDataFrame(resultMap(in.toString), option.asInstanceOf[Map[String, String]])
      }
      case (in, "filter", out, option) => {
        val df = filterDataFrame(resultMap(in.toString), option.asInstanceOf[Map[String, String]])
        
        for (i <- 0 to df.size - 1)
          resultMap += (out.asInstanceOf[List[String]](i) -> df(i))
      }
      case (in, "join", out, option) => {
        //println(option.asInstanceOf[Map[String,List[String]]])
        val df = joinDataFrame(spark,
          resultMap(in.asInstanceOf[List[String]](0)),
          resultMap(in.asInstanceOf[List[String]](1)),
          option.asInstanceOf[Map[String, List[String]]]("keys"),
          option.asInstanceOf[Map[String, List[String]]]("keys"),
          option.asInstanceOf[Map[String, String]]("joinType"), out.asInstanceOf[List[String]])
        //filterDataFrame(resultMap.get(in.toString).get, option.asInstanceOf[Map[String, String]])
        resultMap += (out.asInstanceOf[List[String]](0) -> df._1,
          out.asInstanceOf[List[String]](1) -> df._2,
          out.asInstanceOf[List[String]](2) -> df._3,
          out.asInstanceOf[List[String]](3) -> df._4,
          out.asInstanceOf[List[String]](4) -> df._5)
      }
      case x => println("not matched " + x)
    }
  }

  implicit def list2Tuple4(in: List[Any]): scala.Tuple4[Any, String, Any, Any] = {
    (in(0), in(1).toString, in(2), if (in.size < 4) "" else in(3))
  }
  implicit def jValueV2Tuple4(in: JValue#Values): scala.Tuple4[Any, String, Any, Any] = {
    val inList = in.asInstanceOf[List[String]]
    (inList(0), inList(1).toString(), inList(2), if (inList.size < 4) "" else inList(3))
  }
  implicit def jValue2Tuple4(in: JValue): scala.Tuple4[Any, String, Any, Any] = {
    val inList = in.values.asInstanceOf[List[String]]
    (inList(0), inList(1).toString(), inList(2), if (inList.size < 4) "" else inList(3))
  }

}
