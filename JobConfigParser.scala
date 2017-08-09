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
        val df = READ(spark, 
            option.asInstanceOf[Map[String, String]])
        resultMap += (out.toString() -> df)
      }
      case (in, "write", out, option) => {
        WRITE(resultMap(in.toString), 
            option.asInstanceOf[Map[String, String]])
      }
      case (in, "filter", out, option) => {
        val df = FILTER(resultMap(in.toString), 
            option.asInstanceOf[Map[String, String]])
        out.asInstanceOf[List[String]].zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))
      }
      case (in, "diff", out, option) => {
        val df = DIFF(resultMap(in.asInstanceOf[List[String]](0)), 
            resultMap(in.asInstanceOf[List[String]](1)))
        out.asInstanceOf[List[String]].zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))
      }
      case (in, "join", out, option) => {
        //used in the implicit calls for function getString
        implicit val map = option.asInstanceOf[Map[String, String]]
        val df = JOIN(spark,
          resultMap(in.asInstanceOf[List[String]](0)),
          resultMap(in.asInstanceOf[List[String]](1)),
          option("keys"),
          option("keys"),
          getString("joinType"),
          out.asInstanceOf[List[String]])
        out.asInstanceOf[List[String]].zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))

      }
      case (in, "union", out, option) => {
        val df = UNION(in.asInstanceOf[List[String]].map(resultMap))
        resultMap += (out.toString() -> df)
      }

      case (in, "groupby", out, option) => {
        val df = GROUPBY(resultMap(in.toString), 
            option("gCols"), 
            option("aggCond").asInstanceOf[List[(String, String, String)]])
        resultMap += (out.toString() -> df)
      }

      case (in, "sortby", out, option) => {
        val df = SORT(resultMap(in.toString), 
            option("sortCondList").asInstanceOf[List[(String, String)]])
        resultMap += (out.toString() -> df)
      }

      case (in, "dedup", out, option) => {
        val df = DEDUP(resultMap(in.toString), 
            option("dupCols"), 
            option("deDupConfig").asInstanceOf[Map[String, String]])
        out.asInstanceOf[List[String]].zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))
      }

      case (in, "reformat", out, option) => {
        val df = REFORMAT(resultMap(in.toString), 
            option("reformatCol").asInstanceOf[List[(String, String)]], 
            option("otherOptions").asInstanceOf[Map[String, String]])
        resultMap += (out.toString() -> df)
      }

      case (in, "mreformat", out, option) => {
        val df = MREFORMAT(resultMap(in.toString), 
            option("mReformat").asInstanceOf[Map[String, List[(String, String)]]], 
            option("mReformatCond").asInstanceOf[Map[String, String]], 
            option("otherOptions").asInstanceOf[Map[String, String]])
        df.map(entry => resultMap += (entry._1 -> entry._2))
      }

      case (in, "lookup", out, option) => {
        val df = LOOKUP(resultMap(in.asInstanceOf[List[String]](0)),
          resultMap(in.asInstanceOf[List[String]](1)),
          option("lookupCol").asInstanceOf[String],
          option("lookupKey"))
        resultMap += (out.toString() -> df)
        
      }
      
      case (in, "sequence", out, option) => {
        val df = SEQUENCE (spark, resultMap(in.toString),
          option("startVal").asInstanceOf[Int],
          option("stepVal").asInstanceOf[Int],
          option("targetCol").asInstanceOf[String],
          option("otherOptions").asInstanceOf[Map[String,String]])
        resultMap += (out.toString() -> df)
      }
      
      case (in, "partition", out, option) => {
        val df = PARTITION (resultMap(in.toString),
          option("partitionCols"),
          option("numOfPartitions").asInstanceOf[Int])
        resultMap += (out.toString() -> df)
      }
      
      case (in, "storage", out, option) => {
          STORAGE (option("partitionCols").asInstanceOf[Map[String, String]].map(entry=> (resultMap(entry._1) -> entry._2)))
      }
      
      case (in, "dqvalidator", out, option) => {
        val df = DQVALIDATOR (resultMap(in.toString),
          option("requiredDQInputs"),
          option("stringDQInputs").asInstanceOf[List[(String, Int, Int)]],
          option("intDQInputs").asInstanceOf[List[(String, String)]],
          option("doubleDQInputs").asInstanceOf[List[(String, String)]],
          option("datetimeDQInputs").asInstanceOf[List[(String, String)]])
          out.asInstanceOf[List[String]].zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))
      }
      
      case (in, "limit", out, option) => {
        val df = LIMIT (resultMap(in.toString),
          option("requiredDQInputs").asInstanceOf[String])
          resultMap += (out.toString() -> df)
      }
      
      case (in, "pivot", out, option) => {
        val df = PIVOT (resultMap(in.toString),
          option("keyCol"),
          option("pivotCol").asInstanceOf[String],
          option("aggCondList").asInstanceOf[List[(String, String)]])
          resultMap += (out.toString() -> df)
      }
      
      case (in, "cummulative", out, option) => {
        val df = CUMULATIVE (resultMap(in.toString),
          option("keyCol"),
          option("orderCol"),
          (option("cumlativeFuncCol")(0),option("cumlativeFuncCol")(1),option("cumlativeFuncCol")(2)),
          option("orderCol").asInstanceOf[Char])
          resultMap += (out.toString() -> df)
      }
      
      case (in, "sql", out, option) => {
        val df = SQL (spark, resultMap(in.toString),
          option("sqlStatement").asInstanceOf[String])
          resultMap += (out.toString() -> df)
      }
      
      case (in, "sqle", out, option) => {
        val df = SQLE (spark, 
          option("inputDFs").asInstanceOf[Map[String, String]].map(entry=> (entry._2 -> resultMap(entry._1))),
          option("sqlStatement").asInstanceOf[String])
          resultMap += (out.toString() -> df)
      }
      
      case (in, "scan", out, option) => {
        val df = SCAN (resultMap(in.toString),
          option("keyCol").asInstanceOf[List[String]],
          option("orderCol").asInstanceOf[List[String]],
          (option("FuncColCond")(0),option("FuncColCond")(1),option("FuncColCond")(2),option("FuncColCond")(3)),
          option("orderCol").asInstanceOf[Char])
          resultMap += (out.toString() -> df)
      }
      
      case (in, "noramlize", out, option) => {
        val df = NORMALIZE (resultMap(in.toString),
          option("normalizeCol").asInstanceOf[String],
          option("selCol"))
          resultMap += (out.toString() -> df)
      }
      
 /*     case (in, "noramlizee", out, option) => {
        val df = NORMALIZEE (resultMap(in.toString),
          option("normalizeCol").asInstanceOf[String],
          option("selCol"))
          resultMap += (out.toString() -> df)
      }*/
      
      case (in, "sortWithinPartiotion", out, option) => {
        val df = SORT_PARTITION (resultMap(in.toString),
          option("partitionBy").asInstanceOf[String])
          resultMap += (out.toString() -> df)
      }
      
      case (in, "reformatNew", out, option) => {
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
