package aero.core
package parser

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import EtlFunc._
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

  //ETL main function call
  etlMain

  println("printing the results from result map of size " + resultMap.size)
  resultMap("masterMatchOldFormattedDF").show
  spark.stop()

  def etlMain() = {
    val myJson = parse(scala.io.Source.fromFile("""C:\tmp\test1.json""").mkString)
    println(s"__job has been started__")
    myJson.children.foreach(switchCase(_))
    println(s"__job has been completed__")
  }

  val Test = "(?i)read".r
  def switchCase(confTuple: (Any, String, Any, Any)): Unit = {
    confTuple match {
      case (in, "read", out, option) => {
        val df = readDataFrame(spark, option.asInstanceOf[Map[String, String]])
        resultMap += (out.toString() -> df)
      }
      case (in, "write", out, option) => {
        writeDataFrame(resultMap(in.toString), option.asInstanceOf[Map[String, String]])
      }
      case (in, "filter", out, option) => {
        val df = filterDataFrame(resultMap(in.toString), option.asInstanceOf[Map[String, String]])
        out.asInstanceOf[List[String]].zip(df).foreach(x => resultMap += (x._1 -> x._2))
      }
      case (in, "diff", out, option) => {
        val df = diffDataFrame(resultMap(in.asInstanceOf[List[String]](0)), resultMap(in.asInstanceOf[List[String]](1)))
        out.asInstanceOf[List[String]].zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))
      }
      case (in, "join", out, option) => {
        //used in the implicit calls for function getString
        implicit val map = option.asInstanceOf[Map[String, String]]
        val df = joinDataFrame(spark,
          resultMap(in.asInstanceOf[List[String]](0)),
          resultMap(in.asInstanceOf[List[String]](1)),
          option("keys"),
          option("keys"),
          getString("joinType"),
          out.asInstanceOf[List[String]])
        out.asInstanceOf[List[String]].zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))

      }
      case (in, "union", out, option) => {
        val df = unionDataFrame(in.asInstanceOf[List[String]].map(resultMap): _*)
        resultMap += (out.toString() -> df)
      }
      case (in, "reformat", out, option) => {
        val dropCols = option("drop")
        implicit val inputDF = resultMap(in.toString)
        implicit val xfrmOpts = option.asInstanceOf[Map[String, Any]]
        val df = reformatDFnew
        resultMap += (out.toString() -> df)
      }
      case x => println("not matched " + x)
    }
  }

  //implicit functions for internal conversions 
  implicit def jValue2Tuple4(in: JValue): scala.Tuple4[Any, String, Any, Any] = {
    val inList = in.values.asInstanceOf[List[String]]
    (inList(0), inList(1).toString().toLowerCase(), inList(2), if (inList.size < 4) "" else inList(3))
  }
  implicit def anyToMapLS(in: Any): Map[String, List[String]] = { in.asInstanceOf[Map[String, List[String]]] }

  def getString(in: String)(implicit map: Map[String, String]): String = { map(in) }

  def reformatDFnew()(implicit inputDF: DataFrame, options: Map[String, Any]): DataFrame = {
    val inCols = scala.collection.mutable.ArrayBuffer(inputDF.columns: _*)
    println(inCols.getClass)
    options.asInstanceOf[Map[String, List[Map[String, String]]]].foreach {
      _ match {
        case ("insert", opts) => {
          opts.foreach { x =>
            inCols.insert(Integer.parseInt(x("index")),
              x("function") match {
                case "currentDate" => {
                  val zone = x.getOrElse("zone", "UTC")
                  val format = x.getOrElse("format", "yyyyMMddHHmmss")
                  val col = x("name")
                  if ("UTC".equals(zone))
                    s"""date_format(current_timestamp,"$format") as $col"""
                  else
                    s"""date_format(from_utc_timestamp(current_timestamp,"$zone"),"$format") as $col"""
                }
                case "default" => {
                  val value = x.getOrElse("value", "")
                  val col = x("name")
                  s""""$value" as $col"""
                }
              })
          }
        }
        case ("transform", opts) => {

          opts.foreach { x =>
            inCols.insert(Integer.parseInt(x("index")),
              x("function") match {
                case "rename" => {
                  val from = x("from")
                  val to = x("to")
                  s""" $from as $to"""
                }
                case "substring" => {
                  val from = x("from")
                  val to = x("to")
                  val position = x("position")
                  val length = x("length")
                  s""" substring($from,$position,$length) as $to"""
                }
              })
          }

        }
        case x => println("nothing matched " + x)
      }
    }
    inCols.foreach(println)
    inputDF.selectExpr(inCols: _*).drop(options("drop").asInstanceOf[List[String]]: _*)
    //spark.emptyDataFrame
  }

}
