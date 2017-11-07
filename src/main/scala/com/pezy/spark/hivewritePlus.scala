package com.pezy.spark

import java.util.HashMap
import java.util.Properties
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}

import org.apache.spark.sql.hive.pezyhivewriteif

import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes


/**
  * Created by 冯刚 on 2017/8/18.
  */
class hivewritePlus extends pezyhivewriteif{

  var conf = HBaseConfiguration.create()

  var table = new HTable(conf,"")

  def initHbase(properties: Properties) = {

    val columnsType = properties.get("columns.types").toString.split(":")
    val columns = properties.get("hbase.columns.mapping").toString.split(",")
    val hbaseTableName = properties.get("hbase.table.name").toString
    val zk = properties.get("zk").toString.split(":")
    val zkport = zk(0)
    val zkquorum = zk(1)

    if(table.getConnection==None){
      conf.set("hbase.zookeeper.property.clientPort", zkport)
      conf.set("hbase.zookeeper.quorum", zkquorum)
      conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
      table = new HTable(conf,hbaseTableName)
    }
    (table, columns)
  }
  def initKakfa(properties: Properties)={
    val outputzkQuorum = properties.get("outputzkQuorum").toString
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, outputzkQuorum)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props
  }
  def pezyhivewrite(row:InternalRow,wrappers:Array[(Any)=>Any],dataTypes: Seq[DataType],fieldOIs:Array[ObjectInspector], outputData:Array[Any], standardOI: StructObjectInspector,tableDesc: TableDesc) : Unit = {

    val properties = tableDesc.getProperties
    println(properties)
    if(properties.get("outputdatasource").toString.equals("kafka")){
      val props = initKakfa(properties)
      val outputtopics = properties.get("outputtopics").toString
      val r = row.toString
      sendMessageTokafka(props,outputtopics,r)
    }else{
      val (table, columns) = initHbase(properties)
      //iterator.foreach{ row => {
      var i = 0
      var j = 1
      while (i < fieldOIs.length) {
        outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row.get(i, dataTypes(i)))
        i += 1
      }
      val put = new Put(Bytes.toBytes(outputData(0).toString))
      while(j < i){
        if(outputData(j)!= null){
          put.add(Bytes.toBytes(columns(j).split(":")(0)),Bytes.toBytes(columns(j).split(":")(1)),Bytes.toBytes(outputData(j).toString))
        }
        j +=1
      }
      table.put(put)
    }
  }

  def pezyclose() : Unit = {
    table.close()
  }

  def sendMessageTokafka(props:HashMap[String,Object],topic:String,json:String): Unit ={
    val producer = new KafkaProducer[String, String](props)
    val message = new ProducerRecord[String, String](topic, null, json)
    producer.send(message)
    Thread.sleep(1000)
  }

}
