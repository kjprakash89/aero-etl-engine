package org.capone.etlsparkscala

import org.apache.spark.sql.{Encoder, Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession


object EtlFuncbkp {
  println("__executing ETL function__")
  //read
  def readDataFrame(ss: SparkSession, fileConfig:Map[String,String]): DataFrame = {
    fileConfig("fileFormat") match {
      case "parquet"  => ss.read.parquet(fileConfig("filePath"))
      case "json"     => ss.read.json(fileConfig("filePath"))
      case fileType   => ss.read.format(fileType).options(fileConfig.tail).load(fileConfig("filePath"))        
    }
  }
  //write
  def writeDataFrame(inputDF: DataFrame, writeConfig:Map[String,String]): Int = {
    writeConfig("fileFormat") match {
      case "parquet"  => {inputDF.na.drop("all").repartition(1).write.parquet(writeConfig("filePath")); 0}
      case "json"     => {inputDF.na.drop("all").repartition(1).write.json(writeConfig("filePath")); 0}
      case fileType   => {inputDF.na.drop("all").repartition(1).write.format(fileType).options(writeConfig.tail).save(writeConfig("filePath")); 0}        
    }
  }
  //filter
  def filterDataFrame(inputDF: DataFrame, filterConfig: Map[String,String]): List[DataFrame] =  {
    val filteredDF = inputDF.filter(filterConfig("filterCond"))
    if (filterConfig("deSelect") == "N") List(filteredDF) else List(filteredDF, inputDF.except(filteredDF))   
  } 
  //sort  
  def sortDataFrame(inputDF: DataFrame, sortCond: List[(String, String)]): DataFrame = {  
    inputDF.orderBy(sortCond.map {case (sOrd, sCols) => sOrd match {case "asc" => asc(sCols); case "desc" => desc(sCols)}} : _*)
    //inputDF.orderBy(sortCond.map {case (sOrd, sCols) => sOrd match {case "asc" => expr(sOrd + "(" + sCols + ")"); case "desc" => desc(sCols)}} : _*)
  }
  //dedup
  def deDupDataFrame(inputDF: DataFrame, deDupConfig: Map[String, String]): List[DataFrame] = {
    val dupCols = deDupConfig.getOrElse("deDupCols","").split(",").map(_.trim).toList
    val dupOption = deDupConfig.getOrElse("keep", "")
    val removeDuplicatesDF = removeDuplicates(inputDF, dupCols, dupOption)
    if (deDupConfig.getOrElse("captureDuplicates", "N") == "Y") 
      List(removeDuplicatesDF, inputDF.except(removeDuplicatesDF))
    else 
      List(removeDuplicatesDF)
  }
  
  def removeDuplicates(inputDF: DataFrame, dupCols: List[String], dupOption: String): DataFrame = {    
    dupCols match {
      case _ if dupOption == "unique"  => {
        val selectCols = dupCols.map(col(_))
        inputDF.select(selectCols: _*).except(inputDF.except(inputDF.dropDuplicates(dupCols)).select(selectCols: _*)).join(inputDF, dupCols, "inner")
      }
      case _ if dupOption == "first"   => inputDF.dropDuplicates(dupCols)
      case _ if dupOption == "last"   => {
        val removeDupLastCond = (for (gcols <- inputDF.columns.toList diff (dupCols)) yield (dupOption, gcols, gcols)) 
        groupByDataFrame(inputDF, dupCols, removeDupLastCond).select(inputDF.columns.map(col(_)): _*)
      }
      case Nil => inputDF.dropDuplicates()                                            
      case _  => inputDF.dropDuplicates(dupCols) 
    }   
  }  
  //groupby  
  def groupByDataFrame(inputDF: DataFrame, gCols: List[String], aggCond: List[(String, String, String)]): DataFrame = {
    val gropyByFuncFirst = {x: (String, String, String) => x._1 + '(' + x._2 + ')' + " as " + x._3}  
    val gropyByFuncRest = for (x <- aggCond.tail) yield expr(gropyByFuncFirst(x))
    
    inputDF.groupBy(gCols.map(c => col(c)): _*).agg(expr(gropyByFuncFirst(aggCond.head)),gropyByFuncRest: _* )
  }
  
  def groupByDataFrame(inputDF: DataFrame, 
                       gCols: List[String], 
                       aggCols: List[String], 
                       firstOrLastOption: String): DataFrame = {
    
    val aggFirstOrLastCond = (for (gc <- aggCols diff (gCols)) yield (firstOrLastOption, gc, gc)) 
    groupByDataFrame(inputDF, gCols, aggFirstOrLastCond)    
  }
  //join
  def joinDataFrame(ss: SparkSession, 
                    leftDF: DataFrame, 
                    rightDF: DataFrame, 
                    leftKeyCols: List[String],
                    rightKeyCols: List[String],
                    joinType: String,
                    joinOutput: List[String]): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {  
    println("__In Join __")

    def getJoinOutput(
        leftInputDF: DataFrame,        
        rightColsRenamDF: DataFrame,
        joinCond: Column,
        typeOfJoin: String, 
        jOut:(String, Int)): DataFrame = {
      
      jOut match {        
        case (x, 0) if x != "_" => {leftInputDF.join(rightColsRenamDF, joinCond, typeOfJoin)}
        case (x, 1) if x != "_" => {leftInputDF.join(rightColsRenamDF, joinCond, "leftanti") }          
        case (x, 2) if x != "_" => {rightDF.join(leftInputDF, joinCond, "leftanti")}
        case (x, 3) if x != "_" => {leftInputDF.join(rightDF, joinCond, "leftsemi")}
        case (x, 4) if x != "_" => {rightDF.join(leftInputDF, joinCond, "leftsemi")}
        
        //case (x, 2) if x != "_" => rightDF.except(rightDF.join(leftInputDF,joinCond, "leftsemi"))
        /*case (x, 2) if x != "_" => {
          
          
          rightColsRenamDF.join(leftInputDF, joinCond, "leftanti").show
          println("__end of ETL__")
          rightColsRenamDF.except(leftInputDF.join(rightColsRenamDF, joinCond, "leftanti"))
        }*/
        case _ => ss.emptyDataFrame
      }      
    }
    
    val joinOutList = 
    if (rightKeyCols.isEmpty){
      val rightRenameCols = leftDF.columns.toList intersect rightDF.columns.toList diff leftKeyCols       
      val rightRenamedDF = rightDF.select(leftKeyCols.map(c => col(c)).union(rightRenameCols.map(c => col(c).alias(c + "_Right"))): _*)
      val joinCols = leftKeyCols.zip(leftKeyCols).map { case(key1, key2) => leftDF(key1) === rightRenamedDF(key1)}.reduce(_ && _) 
      for ((y,i) <- joinOutput.zipWithIndex) yield getJoinOutput(leftDF, rightRenamedDF, joinCols, joinType, (y,i))
    }
    else {
      val rightRenameCols = leftDF.columns.toList intersect rightDF.columns.toList diff rightKeyCols 
      val rightRenamedDF = rightDF.select(rightKeyCols.map(c => col(c)).union(rightRenameCols.map(c => col(c).alias(c + "_Right"))): _*)
      val joinCols = leftKeyCols.zip(rightKeyCols).map { case(key1, key2) => leftDF(key1) === rightRenamedDF(key1)}.reduce(_ && _)
      for ((y,i) <- joinOutput.zipWithIndex) yield getJoinOutput(leftDF, rightRenamedDF, joinCols, joinType, (y,i))
    }
    println("i am out")
   (joinOutList(0), joinOutList(1), joinOutList(2), joinOutList(3), joinOutList(4)) 
  }
  //reformat
  def reformatDataFrame(inputDF: DataFrame, reformatCond: List[(String, String)], reformatCols: List[String]): DataFrame = {
    
    val reformatFunc = {x: (String, String) => x._1 + " as " + x._2}    
    //construct reformat function in column order and additional functions at the end
    def constructReformatCond(selCols: List[String], rConf: List[(String, String)]): List[Column]  = {
      selCols match {
        case hCol :: tCols => {
          val reformatFuncCond =rConf.filter((x) => x._1.contains(hCol) || x._2 == hCol)
          //val reformatFuncCond = rConf.filter((x) => x._2 == hCol)
          val reformatColFunction: List[Column] = 
            if (reformatFuncCond.length > 0)
              reformatFuncCond.map(x => expr(reformatFunc(x))) toList
          else 
            List(col(hCol)) 
          reformatColFunction ++ constructReformatCond(tCols, rConf diff reformatFuncCond)
        }
        case Nil => rConf.map(x => expr(reformatFunc(x))) toList       
      }
    }
    
    //reformat main - check select only or reformat only or reformat with Select
    reformatCols  match {
      case "select" :: xs => {
        reformatCond match {
          case Nil =>  if (xs.isEmpty) inputDF.select(inputDF.columns.map(col(_)): _*) else inputDF.select(xs.map(col(_)): _*)
          case _ => {            
              if (xs.isEmpty) 
                inputDF.select(constructReformatCond(inputDF.columns.toList, reformatCond): _*) 
              else {
                //val rColsAll = constructReformatCond(inputDF.columns.toList, reformatCond)
                println("__________unzip____________")
                println(reformatCond.unzip._1.mkString(","))
                val selectColsPart = xs.partition(x => reformatCond.unzip._1.mkString.contains(x))
                val colsFromDF = selectColsPart._1
                val colsFromXform = selectColsPart._2
                println("__________cols____________")
                colsFromDF.foreach(println)
                println("_____________derived__________")
                colsFromXform.foreach(println)
                inputDF.select(constructReformatCond(colsFromXform ++ colsFromDF, reformatCond): _*).select(xs.map(col(_)): _*)
                //val tempDF = inputDF.select(constructReformatCond(inputDF.columns.toList, reformatCond) ++ xs.map(col(_)): _*)
                //reformatDataFrame(tempDF, Nil, reformatCols)                
                //rColsAll.filter(x => rColsSel.mkString.contains(x.toString()))
                //val allCols = inputDF.columns.toList diff xs map(col(_))
                //val rCols = reformatCond.map(x => expr(reformatFunc(x)))
                //inputDF.select((allCols ++ rCols).distinct: _*)
              }
          }
        }
      }
      case "drop" :: xs => {
        reformatCond match {
          case Nil => 
            if (xs.isEmpty) inputDF.select(inputDF.columns.map(col(_)): _*) else inputDF.select(inputDF.columns diff xs map(col(_)): _*)
          case _ => {
            val rCols = constructReformatCond(inputDF.columns.toList, reformatCond)
            val rfmtConds = if (xs.isEmpty) rCols else rCols diff xs.map(col(_))                  
            inputDF.select(rfmtConds: _*)       
            //val dropCols = if (reformatCols.tail.isEmpty) Nil else reformatCols.tail
            }
          }
        }
      case _ => {
        val rCols = inputDF.columns.toList  
        val rfmtConds = constructReformatCond(rCols, reformatCond) 
        inputDF.select(rfmtConds: _*) 
      }
    }
  }
  //diff
  def diffDataFrame(inputDF1: DataFrame, inputDF2: DataFrame): (DataFrame, DataFrame, DataFrame) = {    
    (inputDF1.except(inputDF2), inputDF2.except(inputDF1), inputDF1.intersect(inputDF2))
  }
  //
  //union
  def unionDataFrame(inputDFs: DataFrame*): DataFrame = {
    println("im in union")
    inputDFs.reduce((aDF, bDF) => aDF.union(bDF))
  }
  //lookup
  def lookupDataFrame(inputDF: DataFrame, lookupDF: DataFrame, lookupCol: String, lookupKey: String*): DataFrame = {
    inputDF.join(broadcast(lookupDF), lookupKey, "left").drop(lookupKey: _*)
  }
}
