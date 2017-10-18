package com.pezy.spark

import com.sun.tools.javac.util.Name.Table
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.spark.TaskContext

import org.apache.spark.sql.catalyst.expressions.Attribute

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

  def init(tableDesc: TableDesc) = {
    val properties = tableDesc.getProperties
    println(properties)
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

  def pezyhivewrite(row:InternalRow,wrappers:Array[(Any)=>Any],dataTypes: Seq[DataType],fieldOIs:Array[ObjectInspector], outputData:Array[Any], standardOI: StructObjectInspector,tableDesc: TableDesc) : Unit = {

    val (table, columns) = init(tableDesc)
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
  def pezyclose() : Unit = {
    table.close()
  }

}
