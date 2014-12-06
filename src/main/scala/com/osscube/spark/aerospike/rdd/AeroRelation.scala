package com.osscube.spark.aerospike.rdd

import java.lang

import com.aerospike.client.AerospikeClient
import com.aerospike.client.cluster.Node
import com.aerospike.client.policy.{QueryPolicy, ClientPolicy}
import com.aerospike.client.query.Statement
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{StructType, Row, SQLContext}
import org.apache.spark.sql.sources._
import scala.collection.JavaConverters._
import com.aerospike.client.Info

case class AeroRelation (initialHost: String,
                         select: String,
                         partitionsPerServer : Int = 1)(@transient val sqlContext: SQLContext)
  extends PrunedFilteredScan {

  var schemaCache : StructType = null
  var nodeList: Array[Node] = null
  var namespaceCache: String = null
  var setCache: String = null
  var filterTypeCache: Int = 0
  var filterBinCache: String = null
  var filterValsCache : Seq[(Long, Long)] = null
  var filterStringValCache: String = null

  override def schema: StructType = {
    if(schemaCache == null && nodeList == null) {
      val (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal) = AerospikeRDD.parseSelect(select, partitionsPerServer)
      namespaceCache = namespace
      setCache = set
      filterTypeCache = filterType
      filterBinCache  = filterBin
      filterValsCache = filterVals
      filterStringValCache = filterStringVal
      val policy = new ClientPolicy()
      val splitHost = initialHost.split(":")
      val client = new AerospikeClient(policy, splitHost(0), splitHost(1).toInt)
      try {
        nodeList = client.getNodes()
        val newSt = new Statement()
        newSt.setNamespace(namespace)
        newSt.setSetName(set)
        if (bins.length > 1 || bins(0) != "*")
          newSt.setBinNames(bins: _*)

        val qp: QueryPolicy = new QueryPolicy()
        qp.maxConcurrentNodes = 1
        qp.recordQueueSize = 1

        val recs = client.queryNode(qp, newSt, nodeList(0))
        recs.next()
        val record = recs.getRecord
        val singleRecBins = record.bins
        var binNames = bins
        if (bins.length == 1 && bins(0) == "*") {
          binNames = singleRecBins.keySet.asScala.toSeq
        }
        schemaCache = StructType(binNames.map { b =>
          singleRecBins.get(b) match {
            case v: Integer => StructField(b, IntegerType, true)
            case v: lang.Long => StructField(b, LongType, true)
            case _ => StructField(b, StringType, true)
          }
        })
        recs.close()
      } finally {
        client.close()
      }
    }

    schemaCache
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
  {
    if(schemaCache == null && nodeList == null) {
      val tmp = schema
    }

//    println("Req columns: " + requiredColumns)
//    println("Filters " + filters)

    //Filter types: 0 none, 1 - equalsString, 2 - equalsLong, 3 - range

    if(filterTypeCache == 0 && filters.length > 0) {
      val index_info = Info.request(nodeList(0), "sindex").split(";")

      def checkIndex(attr: String): Boolean = {
        index_info.filter(_.contains("ns=" + namespaceCache + ":set=" + setCache + ":")).filter(_.contains(":bins=" + attr + ":")).length > 0
      }

      def checkDataTypeAndIndex(attr: String): Boolean = {
        (schemaCache(attr).dataType.typeName == "long" || schemaCache(attr).dataType.typeName == "integer") &&
          checkIndex(attr)
      }

      val attrs : Seq[(Int, String, String, Seq[(Long, Long)])] = filters.flatMap{
        case EqualTo(attribute, value) if  checkIndex(attribute) => Seq(if(schemaCache(attribute).dataType.typeName == "string" ) (1, attribute, value.toString, Seq((0L, 0L))) else (2, attribute, "", Seq((value.toString.toLong, 0L))))

        case f @ GreaterThanOrEqual(_, _) if checkDataTypeAndIndex(f.attribute) =>
          Seq( (3, f.attribute, "", Seq(( f.value.toString.toLong,  filters.flatMap{
            case sf @ LessThan(_, _) if sf.attribute == f.attribute =>  Seq(sf.value.toString.toLong)
            case sf @ LessThanOrEqual(_, _)  if sf.attribute == f.attribute =>  Seq(sf.value.toString.toLong)
            case _ => Seq(Long.MaxValue)
        }.min))))

        case f @ GreaterThan(_, _) if checkDataTypeAndIndex(f.attribute)  =>
          Seq( (3, f.attribute, "", Seq(( f.value.toString.toLong,  filters.flatMap{
            case sf @ LessThan(_, _) if sf.attribute == f.attribute =>  Seq(sf.value.toString.toLong)
            case sf @ LessThanOrEqual(_, _)  if sf.attribute == f.attribute =>  Seq(sf.value.toString.toLong)
            case _ => Seq(Long.MaxValue)
          }.min))))


        case f @ LessThanOrEqual(_, _)  if checkDataTypeAndIndex(f.attribute)   =>
          Seq((3, f.attribute, "", Seq(( f.value.toString.toLong,  filters.flatMap{
            case sf @ GreaterThan(_, _)  if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case sf @ GreaterThanOrEqual(_, _)  if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case _ => Seq(Long.MinValue)
        }.max))))

        case f @  LessThan(_, _) if checkDataTypeAndIndex(f.attribute)   =>
          Seq((3, f.attribute, "", Seq(( f.value.toString.toLong,  filters.flatMap{
            case sf @ GreaterThan(_, _)  if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case sf @ GreaterThanOrEqual(_, _)  if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case _ => Seq(Long.MinValue)
          }.max))))

        case _ => Seq()
      }

      if(attrs.length > 0)
      {
        val (filterType, filterBin, filterStringVal, filterVals) = attrs.head
        new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterType, filterBin, filterStringVal, filterVals)
      }
      else new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterTypeCache, filterBinCache, filterStringValCache, filterValsCache)
    }
    else new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterTypeCache, filterBinCache, filterStringValCache, filterValsCache)

  }
}
