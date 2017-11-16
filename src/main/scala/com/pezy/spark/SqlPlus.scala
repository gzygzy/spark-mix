package com.pezy.spark

/**
  * Created by 冯刚 on 2017/8/1.
  */


import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.sql.ResultSetMetaData
import java.util.{HashMap, Properties}
import com.pezy.spark.license.license3j.CheckLicense
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.graphx._

import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.{DoubleType, StructType, StringType, StructField}
import org.apache.spark.sql.{pezyinterface, SparkSession}
import org.apache.spark.sql.Row
import scala.collection.convert.wrapAsJava.bufferAsJavaList
import org.apache.spark.internal.Logging


class SqlPlus extends pezyinterface with Logging{

  // 解析sql
  def analysisSql(sqlText: String,sparkSession:SparkSession): Unit ={

    //license验证
    val checkLicense = new CheckLicense
    val licenseResult = checkLicense.checkResult()
    /*sparkSession.sessionState.conf.setConfString("engine","phoenix")*/
    val map = sparkSession.sqlContext.getAllConfs
    val flag = map.keys.exists(fg =>
    if(fg.toString == "fg"){
      true
    }else{
      false
    })
    if (flag){
      println(sparkSession.sqlContext.getConf("fg")+"===================")
    }

    if(licenseResult.getResult == true) {

      if (sqlText.startsWith("mlearn")) {

        analysisMLearn(sqlText,sparkSession)

      } else if (sqlText.startsWith("graphx")) {

        analysisGraphx(sqlText,sparkSession)

      /*} else if (sparkSession.sessionState.conf.getConfString("engine").equals("pheonix")) {*/
      } else if (sqlText.startsWith("pheonix")) {
        //sqlText.startsWith("phoenix")
        getPhoenixDataFrame(sqlText,sparkSession)

      } else{
        analysisJdbc(sqlText,sparkSession)
      }
    }else{
      println(licenseResult.getErrMsg)
    }

  }


  def analysisJdbc(sqlText: String,sparkSession:SparkSession):Unit = {

      //解析代码
      val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(sqlText)
      val logicalSt = logicalPlan.treeString
      val st = logicalSt.split("\n")

      var s = new Array[String](5)
      var tableName = new String()
      var dbName = new String()

      for(elem <- st) {
        if(elem.contains("InsertIntoTable")&&elem.contains("UnresolvedRelation")){
          s =  elem.split("`")

          if(s.length == 5){
            dbName = s(1)
            tableName = s(3)
          }
          if(s.length == 3){
            dbName = sparkSession.sessionState.catalog.getCurrentDatabase
            tableName = s(1)
          }
        }
        if (elem.contains("UnresolvedRelation")) {
          s = elem.split("`")
          if (s.length == 2) {
            dbName = sparkSession.sessionState.catalog.getCurrentDatabase
            tableName = s(1)
          }
          if (s.length == 4) {
            dbName = s(1)
            tableName = s(3)
          }
          val dbTable: String = dbName + "." + tableName
          //check方法返回option

          val df = sparkSession.sqlContext.emptyDataFrame

          val check = df.lookupPezyTable(dbTable, tableName)

          if (check == None) {
            findExtendMessage(dbName, tableName, sparkSession)
          }
        }
      }

  }

  def analysisMLearn(mlearntext: String , spark: SparkSession): Unit = {

    val sc = spark.sparkContext

    val sqltext = mlearntext.substring(7)

    val dataset = spark.sql(sqltext)
    val length = dataset.columns.length

    val r = dataset.rdd
    r.foreach(s => println(s))
    val parsedData = r.map(s => {
      var arr = new Array[Double](length)
      for(i <-0 to length-1){
        arr(i) = s.get(i).toString.toDouble
      }
      Vectors.dense(arr)
    })
    parsedData.foreach(f=>{println(f)})

    val numClusters = 2
    val numIterations = 20
    val model = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

  }

  def analysisGraphx(graphxtext: String, spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    val sqltext = graphxtext.substring(7)
    val sqls = sqltext.split(";")
    val vertexdf = spark.sql(sqls(0))
    val lines = vertexdf.rdd.map (row => {
      val str = row.getString(0)+" "+row.getString(1)
      str
    })

    val graph = GraphLoader.edgeList(sc, lines)
    /*val graph = GraphLoader.edgeListFile(sc, "/graphx")*/
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = spark.sql(sqls(1)).rdd.map{ line =>
      (line.getString(0).toLong, line.getString(1))
    }
    /*val users = sc.textFile("/users").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }*/
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
    // $example off$
    spark.stop()

  }

  def findExtendMessage(dbName:String,tableName:String,sparkSession: SparkSession): Unit ={

    try{
      val tableMessage = sparkSession.sharedState.externalCatalog.getTable(dbName,tableName)
      val tblProperties = tableMessage.properties
      /*val partition = tableMessage.partitionColumnNames*/
      //分区依据
      val ext = tblProperties.get("ext")

      if(ext!=None){
        val DB = tblProperties.get("DB")

        val cDB = tblProperties.get("cDB")

        val tName = tblProperties.get("tableName")

        val par = tblProperties.get("partition")
        val myDB = sparkSession.sharedState.externalCatalog.getDatabase(dbName)
        val dbProperties = myDB.properties

        var message = new Array[String](3)

        if(cDB != None){
          val value = dbProperties.get(cDB.get)
          println(value.get)
          if(value != None){
            message = value.get.split(" ")
          }
        }else if(DB != None){
          val value = dbProperties.get(DB.get)
          if(value != None){
            message = value.get.split(" ")
          }
        }else{
          throw new Exception("Unknow jdbc type")
        }

        val url = message(0)
        val user = message(1)
        val password = message(2)
        val part = par.get.split(" ")

        val prop = new Properties()
        prop.setProperty("user",user)
        prop.setProperty("password",password)

        val ff = if(!(par.get==" ")){
          sparkSession.sqlContext.read.jdbc(url,tName.get,part,prop)
        }else{
          sparkSession.sqlContext.read.format("jdbc").options(Map("url" -> url,"user" -> user,"password" -> password
            ,"dbtable" -> tName.get)).load()
        }
        ff.registerPezyTable(dbName,tableName)
      }

    }catch {
      case ex : Exception =>{
        return
      }
    }

  }
  def getPhoenixDataFrame(sqltext: String, spark: SparkSession):Unit={

    /*val sqltext = phoenixtext.substring(8)*/
    logInfo(sqltext)

    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val url = "jdbc:phoenix:hadoop232:2181"
    val conn:Connection = DriverManager.getConnection(url)
    val statement:Statement = conn.createStatement()

    val time = System.currentTimeMillis()
    val rs:ResultSet = statement.executeQuery(sqltext)

    logInfo(rs+"+++++++++++++++++++")
    var list:List[Row] = List()
    val md : ResultSetMetaData  = rs.getMetaData
    val columncount = md.getColumnCount
    var columns : Array[String] = new Array[String](columncount)
    logInfo(columns+"=========1==========")
    //将列名组成schema
    for(i <- 0 to columncount-1){
      columns(i) = md.getColumnName(i+1)
    }
    logInfo(columns.toList+"+++++++++++++fff++++++++++++++++")
    val schema = StructType (
      columns.map(fieldName => StructField(fieldName,StringType,true))
    )
    var rowvalues : Array[String] = new Array(columncount)
    /*val N = rs.getRow*/
    /*var n =0
    while (rs.next()){
      n+=1
    }*/
    /*var values : Array[Row] = new Array(n)*/
    val values = new ArrayBuffer[Row]
    logInfo(values+"=====================")
    while (rs.next()) {
      for(i <- 0 to columncount-1){
        rowvalues(i) = rs.getString(i+1)
      }
      val row= Row.fromSeq(rowvalues.toSeq)
      logInfo(row+"===========4==========")
      values += row
    }

    val rows = values.toArray.toList
    logInfo(rows+"=+++++++++++++++++")
    val s:java.util.List[Row] = bufferAsJavaList(rows.toBuffer)
    logInfo(s+"===============3================")
    val df = spark.createDataFrame(s,schema)
    df.show()

    val timeUsed = System.currentTimeMillis() - time
    System.out.println("time " + timeUsed + "mm")

    rs.close()
    statement.close()
    conn.close()

  }

}

/** Case class for converting RDD to DataFrame
  * case class Record(word: String)*/
/*case class Record(label:Int, features: (Int,Vector,Vector))*/
