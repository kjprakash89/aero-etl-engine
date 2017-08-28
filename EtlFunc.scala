package aero.core

import org.apache.spark.sql.{ Encoder, Dataset, DataFrame, Row }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import java.util.Properties

object EtlFunc {
  println("__executing ETL function__")

  val limit = limitDataFrame _
  val getFirstLastDF = groupByDataFrame _

  val READ = readDataFrame _
  val WRITE = writeDataFrame _
  val GROUPBY = groupByDataFrame _
  val FILTER = filterDataFrame _
  val SORT = sortDataFrame _
  val JOIN = joinDataFrame _
  val DEDUP = dedupDataFrame _
  val REFORMAT = reformatDataFrame _
  val MREFORMAT = mReformatDataFrame _
  val DIFF = diffDataFrame _
  val UNION = unionDataFrame _
  val LOOKUP = lookupDataFrame _
  val SEQUENCE = sequenceDataFrame _
  val PARTITION = partitionDataFrame _
  val STORAGE = storageDataFrame _
  val DQVALIDATOR = dqValidatorDataFrame _
  val LIMIT = limitDataFrame _
  val PIVOT = pivotDataFrame _
  val CUMULATIVE = cumulativeDataFrame _
  val SQL = sqlDataFrame _
  val SQLE = sqlDataFrameE _
  val SCAN = scanDataFrame _
  val NORMALIZE = normalizeDataFrame _
  val NORMALIZEE = normalizeDataFrameE _
  val SORT_PARTITION = sortPartitionDataFrame _

  //read
  def readDataFrame(ss: SparkSession, config: Map[String, String]): DataFrame = {
    config("fileFormat") match {
      case "parquet" => ss.read.parquet(config("filePath"))
      case "json"    => ss.read.json(config("filePath"))
      case "jdbc" => {
        if (config("predicates") != null)
          ss.read.jdbc(config("url"), config("table"), config("predicates").split(","), prepareCxnProps(config)) //url, table, properties)
        else if (config("columnName") != null)
          ss.read.jdbc(config("url"), config("table"), config("columnName"),config("lowerBound").toLong, config("upperBound").toLong,config("numPartitions").toInt, prepareCxnProps(config)) //url, table, properties)
        else
          ss.read.jdbc(config("url"), config("table"), prepareCxnProps(config)) 
      }
      case fileType => ss.read.format(fileType).options(config -- List("fileFormat", "filePath")).load(config("filePath"))

    }
  }

  private def prepareCxnProps(config: Map[String, String]) = {
    val props = new Properties()
    config.iterator.foreach(entry => props.setProperty(entry._1, entry._2))
    props
  }

  //write
  def writeDataFrame(inputDF: DataFrame, writeConfig: Map[String, String]): Int = {
    writeConfig("fileFormat") match {
      case "parquet" => { inputDF.repartition(1).write.parquet(writeConfig("filePath")); 0 }
      case "json"    => { inputDF.repartition(1).write.json(writeConfig("filePath")); 0 }
      case fileType => {
        inputDF
          .repartition(1)
          .write.format(fileType)
          .options(writeConfig -- List("fileFormat", "filePath"))
          .save(writeConfig("filePath")); 0
      }
    }
  }

  //filter
  def filterDataFrame(inputDF: DataFrame, filterConfig: Map[String, String]): (DataFrame, DataFrame) = {
    val filterCondition = filterConfig("filterCond")
    val filteredDF =
      filterCondition match {
        case "head" => inputDF.limit(1)
        case "tail" => {
          val getLastCond = (for (colsDF <- inputDF.columns.toList) yield ("last", colsDF, colsDF))
          getFirstLastDF(inputDF, Nil, getLastCond)
        }
        case _ => inputDF.filter(filterConfig("filterCond"))
      }
    if (filterConfig.getOrElse("deSelect", "N") == "N") (filteredDF, filteredDF.limit(0)) else (filteredDF, inputDF.except(filteredDF))
  }

  //sort  
  def sortDataFrame(inputDF: DataFrame, sortCondList: List[(String, String)]): DataFrame = {
    val sortCond =
      sortCondList.map {
        case (sCols, sOrd) => sOrd match {
          case "asc"              => asc(sCols)
          case "desc"             => desc(sCols)
          case "asc_nulls_first"  => asc_nulls_first(sCols)
          case "asc_nulls_last"   => asc_nulls_last(sCols)
          case "desc_nulls_first" => desc_nulls_first(sCols)
          case "desc_nulls_last"  => desc_nulls_last(sCols)
        }
      }
    inputDF.orderBy(sortCond: _*)
    //inputDF.orderBy(sortCond.map {case (sCols, sOrd) => expr(sCols + sOrd)} : _*)
    //inputDF.orderBy(sortCond.map {case (sOrd, sCols) => sOrd match {case "asc" => expr(sOrd + "(" + sCols + ")"); case "desc" => desc(sCols)}} : _*)
  }

  //dedup
  def dedupDataFrame(inputDF: DataFrame, dupCols: List[String], deDupConfig: Map[String, String]): (DataFrame, DataFrame) = {
    val dupOption = deDupConfig.getOrElse("keep", null)
    val removeDuplicatesDF = removeDuplicates(inputDF, dupCols, dupOption)
    if (deDupConfig.getOrElse("captureDuplicates", "N") == "Y")
      (removeDuplicatesDF, inputDF.except(removeDuplicatesDF))
    else
      (removeDuplicatesDF, removeDuplicatesDF.limit(0))
  }

  def removeDuplicates(inputDF: DataFrame, dupCols: List[String], dupOption: String): DataFrame = {
    dupCols match {
      case _ if dupOption == "unique" => {
        val selectCols = dupCols.map(col(_))
        inputDF.select(selectCols: _*)
          .except(
            inputDF.except(inputDF.dropDuplicates(dupCols))
              .select(selectCols: _*))
          .join(inputDF, dupCols, "inner")
      }
      case _ if dupOption == "first" => inputDF.dropDuplicates(dupCols)
      case _ if dupOption == "last" => {
        val removeDupLastCond = (for (gcols <- inputDF.columns.toList diff (dupCols)) yield (dupOption, gcols, gcols))
        getFirstLastDF(inputDF, dupCols, removeDupLastCond)
      }
      case Nil => inputDF.dropDuplicates()
      case _   => inputDF.dropDuplicates(dupCols)
    }
  }

  //groupby
  def groupByDataFrame(inputDF: DataFrame, gCols: List[String], aggCond: List[(String, String, String)]): DataFrame = {

    val aggCondFinal =
      aggCond.head match {
        case (gFunc, "*", "*") => {
          for (gcols <- inputDF.columns.toList diff (gCols)) yield (gFunc, gcols, gcols)
        }
        case (gFunc, "*", aliasPattern) => {
          val patternSuffixAlias = "\\* \\w+".r
          val suffixAlias = patternSuffixAlias.findFirstIn(aliasPattern).getOrElse("").toString()
          val patternPrefixAlias = "\\w+ \\*".r
          val prefixAlias = patternPrefixAlias.findFirstIn(aliasPattern).getOrElse("").toString()

          if (aliasPattern == prefixAlias) {
            val prefixCol = prefixAlias.split(" ")(0)
            for (gcols <- inputDF.columns.toList diff (gCols)) yield (gFunc, gcols, prefixCol + gcols)
          } else {
            val suffixCol = suffixAlias.split(" ")(1)
            for (gcols <- inputDF.columns.toList diff (gCols)) yield (gFunc, gcols, gcols + suffixCol)
          }
        }
        case _ => aggCond
      }

    val gropyByFuncFirst = { x: (String, String, String) => x._1 + '(' + x._2 + ')' + " as " + x._3 }
    val gropyByFuncRest = for (x <- aggCondFinal.tail) yield expr(gropyByFuncFirst(x))

    inputDF.groupBy(gCols.map(c => col(c)): _*).agg(expr(gropyByFuncFirst(aggCondFinal.head)), gropyByFuncRest: _*)
  }

  //join
  def joinDataFrame(ss: SparkSession,
                    leftDF: DataFrame,
                    rightDF: DataFrame,
                    leftKeyCols: List[String],
                    rightKeyCols: List[String],
                    joinType: String,
                    joinOutput: List[String]): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    def getJoinOutput(
      leftInputDF: DataFrame,
      rightColsRenamDF: DataFrame,
      joinCond: Column,
      typeOfJoin: String,
      jOut: (String, Int)): DataFrame = {

      jOut match {
        case (x, 0) if x != "_" => leftInputDF.join(rightColsRenamDF, joinCond, typeOfJoin)
        case (x, 1) if x != "_" => leftInputDF.join(rightColsRenamDF, joinCond, "leftanti")
        case (x, 2) if x != "_" => rightDF.join(leftInputDF, joinCond, "leftanti")
        case (x, 3) if x != "_" => leftInputDF.join(rightDF, joinCond, "leftsemi")
        case (x, 4) if x != "_" => rightDF.join(leftInputDF, joinCond, "leftsemi")
        case _                  => ss.emptyDataFrame
      }
    }

    val joinOutList =
      if (rightKeyCols.isEmpty) {
        val rightRenameCols = leftDF.columns.toList intersect rightDF.columns.toList diff leftKeyCols
        val rightRenamedDF = rightDF.select(leftKeyCols.map(c => col(c)).union(rightRenameCols.map(c => col(c).alias(c + "_Right"))): _*)
        val joinCols = leftKeyCols.zip(leftKeyCols).map { case (key1, key2) => leftDF(key1) <=> rightRenamedDF(key1) }.reduce(_ && _)
        for ((y, i) <- joinOutput.zipWithIndex) yield getJoinOutput(leftDF, rightRenamedDF, joinCols, joinType, (y, i))
      } else {
        val rightRenameCols = leftDF.columns.toList intersect rightDF.columns.toList diff rightKeyCols
        val rightRenamedDF = rightDF.select(rightKeyCols.map(c => col(c)).union(rightRenameCols.map(c => col(c).alias(c + "_Right"))): _*)
        val joinCols = leftKeyCols.zip(rightKeyCols).map { case (key1, key2) => leftDF(key1) <=> rightRenamedDF(key1) }.reduce(_ && _)
        for ((y, i) <- joinOutput.zipWithIndex) yield getJoinOutput(leftDF, rightRenamedDF, joinCols, joinType, (y, i))
      }
    (joinOutList(0), joinOutList(1), joinOutList(2), joinOutList(3), joinOutList(4))
  }

  //Refomat 
  val reformatExpr = { x: (String, String) => x._1 + " as " + x._2 }

  val dropColumns = {
    rCols: List[(String, String)] =>
      {
        val dropColExpr = rCols.filter { case (rexpr, rcol) => (rcol == "") }
        val dCols = dropColExpr.map { case (rexpr, rcol) => rexpr }
        (dropColExpr, dCols)
      }
  }

  def constructSelectExpr(DFCols: List[String], columnsReformat: List[(String, String)]): List[String] = {
    val dropReformat = dropColumns(columnsReformat)
    val finalTransformExpr = columnsReformat.diff(dropReformat._1)
    val reformatKeyVal = finalTransformExpr.map { case (rexpr, rcol) => (rcol, rexpr) }.toList.toMap
    val existingCols = DFCols.diff(dropReformat._2).map(c => (reformatKeyVal.getOrElse(c, c), c))
    val newCols = finalTransformExpr.map { case (rexpr, rcol) => rcol }.diff(DFCols).map(c => (reformatKeyVal.getOrElse(c, c), c))
    existingCols ++ newCols map { case (rexpr, rcol) => reformatExpr(rexpr, rcol) }
  }

  //core reformat
  def reformatDataFrame(
    inputDF: DataFrame,
    reformatCol: List[(String, String)],
    otherOptions: Map[String, String]): DataFrame = {

    val reformatHeader = reformatCol.head
    val dataFrameCols = inputDF.columns.toList
    val restReformatCols = reformatCol.tail

    val patternSuffixAlias = "as \\* \\w+".r
    val suffixAlias = patternSuffixAlias.findFirstIn(reformatHeader._2).getOrElse("").toString()
    val patternPrefixAlias = "as \\w+ \\*".r
    val prefixAlias = patternPrefixAlias.findFirstIn(reformatHeader._2).getOrElse("").toString()

    val outputDF =
      reformatHeader match {
        case ("*", "*") => inputDF.selectExpr(constructSelectExpr(dataFrameCols, restReformatCols): _*)
        case ("trim(*)", "*") => {
          val stringTrimCols = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr("trim(" + c + ")", c)))
          inputDF.selectExpr(stringTrimCols: _*).selectExpr(constructSelectExpr(dataFrameCols, restReformatCols): _*)
        }
        case _ if reformatHeader._2 == suffixAlias => {
          val aliasList = suffixAlias.split(" ")
          val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c, c + aliasList(2))))
          val aliasDF = inputDF.selectExpr(aliasReformatExpr: _*)
          aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList, restReformatCols): _*)
        }
        case _ if reformatHeader._2 == prefixAlias => {
          val aliasList = prefixAlias.split(" ")
          val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c, aliasList(1) + c)))
          val aliasDF = inputDF.selectExpr(aliasReformatExpr: _*)
          aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList, restReformatCols): _*)
        }
        case _ => {
          val selectReformatExpr = reformatCol.filter { case (rexpr, rcol) => rcol != "" } map { case (rexpr, rcol) => reformatExpr(rexpr, rcol) }
          inputDF.selectExpr(selectReformatExpr: _*)
        }
      }

    applyOtherOptionsDF(outputDF, otherOptions)

    val limitFlag = otherOptions.getOrElse("limit", "N")
    val lastFlag = otherOptions.getOrElse("last", "N")

    if ((limitFlag == "N" | limitFlag == "") && lastFlag == "Y") {
      val outDFCols = outputDF.columns.toList
      val getLastCond = (for (colsDF <- outDFCols) yield ("last", colsDF, colsDF))
      getFirstLastDF(outputDF, Nil, getLastCond)
    } else limit(outputDF, limitFlag)
  }

  //multi reformat
  def mReformatDataFrame(
    inputDF: DataFrame,
    mReformat: Map[String, List[(String, String)]],
    mReformatCond: Map[String, String],
    otherOptions: Map[String, String]): Map[String, DataFrame] = {

    val dataFrameCols = inputDF.columns.toList

    for ((rfmtkey, reformatCol) <- mReformat) yield {

      val reformatHeader = reformatCol.head
      val restReformatCols = reformatCol.tail

      val patternSuffixAlias = "as \\* \\w+".r
      val suffixAlias = patternSuffixAlias.findFirstIn(reformatHeader._2).getOrElse("").toString()
      val patternPrefixAlias = "as \\w+ \\*".r
      val prefixAlias = patternPrefixAlias.findFirstIn(reformatHeader._2).getOrElse("").toString()

      val outputDF =
        reformatHeader match {
          case ("*", "*") => inputDF.selectExpr(constructSelectExpr(dataFrameCols, restReformatCols): _*)
          case ("trim(*)", "*") => {
            val stringTrimCols = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr("trim(" + c + ")", c)))
            if (mReformatCond.getOrElse(rfmtkey, "N") == "N")
              inputDF.selectExpr(stringTrimCols: _*).selectExpr(constructSelectExpr(dataFrameCols, restReformatCols): _*)
            else
              inputDF.filter(mReformatCond.getOrElse(rfmtkey, "N")).selectExpr(stringTrimCols: _*).selectExpr(constructSelectExpr(dataFrameCols, restReformatCols): _*)
          }
          case _ if reformatHeader._2 == suffixAlias => {
            val aliasList = suffixAlias.split(" ")
            val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c, c + aliasList(2))))
            val aliasDF =
              if (mReformatCond.getOrElse(rfmtkey, "N") == "N")
                inputDF.selectExpr(aliasReformatExpr: _*)
              else
                inputDF.filter(mReformatCond.getOrElse(rfmtkey, "N")).selectExpr(aliasReformatExpr: _*)
            aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList, restReformatCols): _*)
          }
          case _ if reformatHeader._2 == prefixAlias => {
            val aliasList = prefixAlias.split(" ")
            val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c, aliasList(1) + c)))
            val aliasDF =
              if (mReformatCond.getOrElse(rfmtkey, "N") == "N")
                inputDF.selectExpr(aliasReformatExpr: _*)
              else
                inputDF.filter(mReformatCond.getOrElse(rfmtkey, "N")).selectExpr(aliasReformatExpr: _*)
            aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList, restReformatCols): _*)
          }
          case _ => {
            val selectReformatExpr = reformatCol.filter { case (rexpr, rcol) => rcol != "" } map { case (rexpr, rcol) => reformatExpr(rexpr, rcol) }
            if (mReformatCond.getOrElse(rfmtkey, "N") == "N")
              inputDF.selectExpr(selectReformatExpr: _*)
            else
              inputDF.filter(mReformatCond.getOrElse(rfmtkey, "N")).selectExpr(selectReformatExpr: _*)

          }
        }

      applyOtherOptionsDF(outputDF, otherOptions)

      val limitFlag = otherOptions.getOrElse("limit", "N")
      val lastFlag = otherOptions.getOrElse("last", "N")

      if ((limitFlag == "N" | limitFlag == "") && lastFlag == "Y") {
        val outDFCols = outputDF.columns.toList
        val getLastCond = (for (colsDF <- outDFCols) yield ("last", colsDF, colsDF))
        (rfmtkey, getFirstLastDF(outputDF, Nil, getLastCond))
      } else (rfmtkey, limit(outputDF, limitFlag))
    }
  }

  //limit
  def limitDataFrame(inputDF: DataFrame, limitOption: String): DataFrame = {
    if (limitOption != "N" && limitOption != "") inputDF.limit(limitOption.toInt) else inputDF
  }

  //diff
  def diffDataFrame(inputDF1: DataFrame, inputDF2: DataFrame): (DataFrame, DataFrame, DataFrame) = {
    (inputDF1.except(inputDF2), inputDF2.except(inputDF1), inputDF1.intersect(inputDF2))
  }

  //union
  def unionDataFrame(inputDFs: DataFrame*): DataFrame = {
    inputDFs.reduce((aDF, bDF) => aDF.union(bDF))
  }

  //lookup
  def lookupDataFrame(inputDF: DataFrame, lookupDF: DataFrame, lookupCol: String, lookupKey: String*): DataFrame = {
    inputDF.join(broadcast(lookupDF), lookupKey, "left").drop(lookupKey: _*)
  }

  //normalize
  def normalizeDataFrame(inputDF: DataFrame, normalizeCol: String, selCol: List[String]): DataFrame = {
    val norSelCols = selCol.map(col(_))
    inputDF.withColumn(normalizeCol, explode(col(normalizeCol))).select(norSelCols: _*)
  }

  def normalizeDataFrameE(
    inputDF: DataFrame,
    rootSelCol: List[String],
    flattenCol: List[String],
    norSchema: Map[String, StructType]): DataFrame = {

    //flattens schema at field level
    def flattenFieldLevel(
      jDF: DataFrame,
      rootCol: List[String],
      flatCol: String,
      flattenSchema: StructType): DataFrame = {

      jDF.selectExpr(rootCol: _*)
        .withColumn(
          flatCol.split('.').last, explode(when((col(flatCol).isNotNull and size(col(flatCol)).gt(0)), col(flatCol))
            .otherwise(array(lit(null).cast(flattenSchema)))))
    }

    flattenCol match {
      case fCol :: Nil => {
        flattenFieldLevel(inputDF, rootSelCol, fCol, norSchema.getOrElse(fCol, null))
      }
      case fCol :: tCol => {
        normalizeDataFrameE(
          flattenFieldLevel(inputDF, rootSelCol, fCol, norSchema.getOrElse(fCol, null)),
          List(fCol),
          tCol,
          norSchema)
      }
      case _ => inputDF.limit(0)
    }
  }

  //pivot
  def pivotDataFrame(
    inputDF: DataFrame,
    keyCol: List[String],
    pivotCol: String,
    aggCondList: List[(String, String)]): DataFrame = {

    val aggCond = aggCondList.map { case (pFunc, tCol) => pFunc + " as " + tCol }
    inputDF.groupBy(keyCol.map(expr(_)): _*).pivot(pivotCol).agg(expr(aggCond.head), aggCond.tail.map(expr(_)): _*)
  }

  //cumulative
  def cumulativeDataFrame(
    inputDF: DataFrame,
    keyCol: List[String],
    orderCol: List[String],
    cumlativeFuncCol: (String, String, String),
    dropCumCol: Char): DataFrame = {

    val cumPartition = Window.partitionBy(keyCol.map(col(_)): _*).orderBy(orderCol.map(col(_)): _*)
    val cumString = cumlativeFuncCol._1 + '(' + cumlativeFuncCol._2 + ')'
    inputDF.withColumn(
      if (cumlativeFuncCol._3 > " ") cumlativeFuncCol._3 else cumlativeFuncCol._1 + cumlativeFuncCol._2, expr(cumString).over(cumPartition))
      .drop(if (dropCumCol == 'Y') cumlativeFuncCol._2 else "")
  }

  //scan
  def scanDataFrame(
    inputDF: DataFrame,
    keyCol: List[String],
    orderCol: List[String],
    FuncColCond: (String, String, String, String),
    dropCumCol: Char): DataFrame = {

    val cumPartition = Window.partitionBy(keyCol.map(col(_)): _*).orderBy(orderCol.map(col(_)): _*)
    val scanCol = if (FuncColCond._4 > " ") FuncColCond._4 else FuncColCond._1 + FuncColCond._2
    val scanString = FuncColCond._2 + " " + FuncColCond._3 + " " + scanCol
    val cumDF = cumulativeDataFrame(inputDF, keyCol, orderCol, (FuncColCond._1, FuncColCond._2, FuncColCond._4), dropCumCol)
    val filterExpr = scanCol + "Flag" + " is null or " + scanCol + " = true"
    REFORMAT(cumDF, List(("*", "*"), (scanString, scanCol + "Flag")), Map("" -> "")).filter(filterExpr).drop(scanCol + "Flag")
  }

  //SQL statement interface
  def sqlDataFrame(ss: SparkSession, inputDF: DataFrame, sqlStatement: String): DataFrame = {
    inputDF.createOrReplaceTempView("inputDFTemp")
    val sqlString = sqlStatement.replaceAll("<table>", "inputDFTemp")
    ss.sql(sqlString)
  }

  //SQL statement extended interface
  def sqlDataFrameE(ss: SparkSession, inputDFs: Map[String, DataFrame], sqlStatement: String): DataFrame = {
    inputDFs.map { case (k, v) => v.createOrReplaceTempView(k) }
    ss.sql(sqlStatement)
  }

  //apply other functions on dataframe
  def applyOtherOptionsDF(outputDF: DataFrame, options: Map[String, String]) = {
    if (options.getOrElse("show", "N") == "N") () else outputDF.show(options.getOrElse("show", "20").toInt, false)
    if (options.getOrElse("printSchema", "N") == "Y") outputDF.printSchema()
    if (options.getOrElse("dtypes", "N") == "Y") {
      println("===> Begin dtype ")
      outputDF.dtypes.foreach(println)
      println("<=== End dtype ")
    }
    if (options.getOrElse("count", "N") == "Y") {
      println("__record count__ " + outputDF.count)
    }
    if (options.getOrElse("explain", "N") == "Y") outputDF.explain
    if (options.getOrElse("dfCountOnColumn", "N") == "N") ()
    else {
      val countCol = options.getOrElse("dfCountOnColumn", "").toString()
      val countExpr = "count(" + countCol + ") as " + "count_" + countCol
      outputDF.selectExpr(countExpr).show
    }
  }

  //sequence generator 
  def sequenceDataFrame(
    ss: SparkSession,
    inputDF: DataFrame,
    startVal: Int,
    stepVal: Int,
    targetCol: String,
    otherOptions: Map[String, String]): DataFrame = {
    val seqColName = if (targetCol == Nil) "seqNum" else targetCol
    val tempRDD = inputDF.rdd.zipWithIndex.map(x => Row(x._2 :: x._1.toSeq.toList: _*))
    val auxSchema = StructType(Array(StructField(seqColName, LongType, false)))
    val sch = StructType(auxSchema ++ inputDF.schema)
    val outputDF = ss.createDataFrame(tempRDD, sch).withColumn(seqColName, col(seqColName) * stepVal + startVal)
    applyOtherOptionsDF(outputDF, otherOptions)
    val limitFlag = otherOptions.getOrElse("limit", "N")
    limit(outputDF, limitFlag)
  }

  //partition DataFrame
  def partitionDataFrame(inputDF: DataFrame, partitionCols: List[String], numOfPartitions: Int): DataFrame = {
    if (numOfPartitions == 0) {
      inputDF.repartition(partitionCols.map(expr(_)): _*)
    } else {
      inputDF.repartition(numOfPartitions, partitionCols.map(expr(_)): _*)
    }
  }

  //sortPartition DataFrame
  def sortPartitionDataFrame(inputDF: DataFrame, partitionBy: String): DataFrame = {
    inputDF.sortWithinPartitions(partitionBy)
  }

  //storage DataFrame
  def storageDataFrame(storageOptions: Map[DataFrame, String]) = {
    for ((inputDF, sOpt) <- storageOptions) {
      sOpt match {
        case "cache"     => inputDF.cache()
        case "persist"   => inputDF.persist()
        case "unpersist" => inputDF.unpersist()
        case _           => ()
      }
    }
  }

  //dqvalidator
  val isColumnRequired = {
    dqString: String =>
      {
        val colName = dqString
        val caseExpr =
          "case when " + colName + " is not null then '*'" +
            " else " + "'" + "++ invalid " + colName + "'" +
            " end as " + "string" + colName + "dqFlag"
        (colName, caseExpr)
      }
  }

  val stringDQ = {
    dqString: (String, Int, Int, Boolean) =>
      {
        val colName = dqString._1
        val minLen = dqString._2
        val maxLen = dqString._3
        val isColumnRequired = dqString._4

        val caseExpr =
          if (isColumnRequired) {
            "case when " + colName + " is not null and " +
              "(length(trim(" + colName + ")) >= " + minLen + " and length(trim(" + colName + ")) <= " + maxLen + ") then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          } else {
            "case when " + colName + " is null then '*'" +
              " when (length(trim(" + colName + ")) >= " + minLen + " and length(trim(" + colName + ")) <= " + maxLen + ") then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + "string" + colName + "dqFlag"
          }
        (colName, caseExpr)
      }
  }

  val intDQ = {
    dqString: (String, String, Boolean) =>
      {
        val colName = dqString._1
        val dataType = dqString._2
        val isColumnRequired = dqString._3

        val caseExpr =
          if (isColumnRequired) {
            "case when " + colName + " is not null and cast(" + colName + ") as " + dataType + ") is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          } else {
            "case when " + colName + " is null then '*'" +
              " when cast(trim(" + colName + ") as " + dataType + ") is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          }
        (colName, caseExpr)
      }
  }

  val doubleDQ = {
    dqString: (String, String, Boolean) =>
      {
        val colName = dqString._1
        val colAttr = dqString._2
        val isColumnRequired = dqString._3

        val caseExpr =
          if (isColumnRequired) {
            "case when " + colName + " is not null and cast(" + colName + " as Decimal(" + colAttr + ")) is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          } else {
            "case when " + colName + " is null then '*'" +
              " when cast(" + colName + " as Decimal(" + colAttr + ")) is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          }
        (colName, caseExpr)
      }
  }

  val datetimeDQ = {
    dqString: (String, String, Boolean) =>
      {
        val colName = dqString._1
        val dataORtimestamp = dqString._2
        val isColumnRequired = dqString._3

        val caseExpr =
          if (isColumnRequired) {
            "case when " + colName + " is not null and cast(" + colName + " as " + dataORtimestamp + ") is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          } else {
            "case when " + colName + " is null then '*'" +
              " when cast(" + colName + " as " + dataORtimestamp + ") is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          }
        (colName, caseExpr)
      }
  }

  val constructSelExpr: List[(String, String)] => List[String] =
    dqConst => {
      dqConst match {
        case (head :: tail) => List(head._1, head._2) ++ constructSelExpr(tail)
        case _              => List()
      }
    }

  val getValidRejectDF = {
    (inputDF: DataFrame, dqCols: List[String], dqExpr: List[String]) =>
      {

        val dfCols = inputDF.columns.toList
        val nonDQCols = dfCols diff dqCols
        val dqDF = inputDF.selectExpr(nonDQCols ++ dqExpr: _*)
        val dqFlagCols = dqDF.columns.toList.filter(_.endsWith("dqFlag"))
        val validCond = dqFlagCols.map(_ + """ = "*"""").mkString(" and ")
        val concateExpr = dqFlagCols.mkString("translate(concat(", ",", "),'*','') as rejectReason")
        val validFlagDF = dqDF.filter(validCond)
        val rejectFlagDF = dqDF.except(validFlagDF)

        (validFlagDF.selectExpr(dfCols: _*), rejectFlagDF.selectExpr(dfCols :+ concateExpr: _*))
      }
  }

  def dqValidatorDataFrame(
    inputDF: DataFrame,
    requiredDQInputs: List[String],
    stringDQInputs: List[(String, Int, Int)],
    intDQInputs: List[(String, String)],
    doubleDQInputs: List[(String, String)],
    datetimeDQInputs: List[(String, String)]): (DataFrame, DataFrame) = {

    val (dqCols, dqSelectExpr) =
      if (requiredDQInputs.nonEmpty && stringDQInputs.isEmpty && intDQInputs.isEmpty && doubleDQInputs.isEmpty && datetimeDQInputs.isEmpty) {
        (requiredDQInputs, constructSelExpr(requiredDQInputs.map(c => isColumnRequired(c))))
      } else {
        val stringDQCols = stringDQInputs.map { case (c, min, max) => c } toList
        val stringDQSelect = stringDQInputs.map { case (c, min, max) => stringDQ(c, min, max, requiredDQInputs.contains(c)) }

        val intDQCols = intDQInputs.map { case (c, dtype) => c } toList
        val intDQSelect = intDQInputs.map { case (c, dtype) => intDQ(c, dtype, requiredDQInputs.contains(c)) }

        val doubleDQCols = doubleDQInputs.map { case (c, colSize) => c } toList
        val doubleDQSelect = doubleDQInputs.map { case (c, colSize) => doubleDQ(c, colSize, requiredDQInputs.contains(c)) }

        val datetimeDQCols = datetimeDQInputs.map { case (c, dtype) => c } toList
        val datetimeDQSelect = datetimeDQInputs.map { case (c, dtype) => datetimeDQ(c, dtype, requiredDQInputs.contains(c)) }

        val dqCols = stringDQCols ++ intDQCols ++ doubleDQCols ++ datetimeDQCols
        val dqSelectExpr = constructSelExpr(stringDQSelect ++ intDQSelect ++ doubleDQSelect ++ datetimeDQSelect)
        (dqCols, dqSelectExpr)
      }
    getValidRejectDF(inputDF, dqCols, dqSelectExpr)
  }
}
