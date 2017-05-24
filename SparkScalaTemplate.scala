package org.capone.etlsparkscala

import EtlFunc._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Dataset, DataFrame}
import org.apache.spark.sql.Column
import org.apache.log4j.{Level, Logger}
import scala.io.Source

object SparkScalaTemplate extends App{
  
  val spark = SparkSession
            .builder()
            .master("local")
            .appName("etl")
            .config("spark.driver.allowMultipleContexts", "true").config("spark.ui.port", "44040") 
            .getOrCreate()
            
  val rootLogger = Logger.getRootLogger() 
  rootLogger.setLevel(Level.ERROR)
  
  println(s"__job has been started__")
 
1234567890
  
  println(s"__job has been completed__")
  spark.stop
}