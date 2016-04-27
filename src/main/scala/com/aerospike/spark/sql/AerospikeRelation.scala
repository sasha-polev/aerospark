package com.aerospike.spark.sql
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.Logging

import scala.collection.mutable.StringBuilder
import scala.collection.JavaConversions._

import com.aerospike.spark.sql.AerospikeConfig._

import com.aerospike.client.cluster.Node
import com.aerospike.client.policy.QueryPolicy
import com.aerospike.client.query.{RecordSet, Statement}
import com.aerospike.client.{AerospikeClient, Info}


case class AerospikeRelation(
  config: AerospikeConfig)
  (@transient val sqlContext: SQLContext) extends BaseRelation
    with PrunedFilteredScan with Logging{

  @transient val queryEngine = AerospikeConnection.getQueryEngine(config)

  var schemaCache: StructType = StructType(Seq.empty[StructField])

  override def schema: StructType = {

    if (schemaCache.isEmpty) {
      val columnTypes = mapAsScalaMap(queryEngine.inferSchema(config.get(AerospikeConfig.NameSpace).toString(),
          config.get(AerospikeConfig.SetName).toString(), 100))
          
    for ((k,v) <- columnTypes) printf("key: %s, value: %s\n", k, v)
     
      val schemaFields = columnTypes.map { b =>
        b._2 match {
          case v: Class[Long] => StructField(b._1, LongType, nullable = true)
          case v: Class[Double] => StructField(b._1, DoubleType, nullable = true)
          case v: Class[String] => StructField(b._1, StringType, nullable = true)
//          case v: Class[List] => StructField(b._1, ListType, nullable = true)
//          case v: Class[Map] => StructField(b._1, MapType, nullable = true)
          case _ => StructField(b._1, StringType, nullable = true)
        }
      }
      schemaCache = StructType(schemaFields.toArray)

    }
    schemaCache
  }
  
  override def buildScan(
    requiredColumns: Array[String],
    filters: Array[Filter]): RDD[Row] = {
     if (filters.length > 0) {
      val index_info = Info.request(nodeList.head, "sindex").split(";")

      def checkIndex(attr: String): Boolean = {
        index_info
          .filter(_.contains("ns=" + namespaceCache + ":set=" + setCache + ":"))
          .filter(_.contains(":bins=" + attr + ":"))
          .length > 0
      }

      def checkNumeric(attr: String): Boolean = {
        val attrType: String = schemaCache(attr).dataType.typeName
        attrType == "long" || attrType == "integer"
      }

      def tupleGreater(attribute: String, value: Any): Seq[(SparkFilterType, String, String, Seq[(Long, Long)])] = {
        Seq((FilterRange, attribute, "", Seq((value.toString.toLong, filters.flatMap {
          case LessThan(sf_attribute, sf_value) if sf_attribute == attribute => Seq(sf_value.toString.toLong)
          case LessThanOrEqual(sf_attribute, sf_value) if sf_attribute == attribute => Seq(sf_value.toString.toLong)
          case _ => Seq(Long.MaxValue)
        }.min))))
      }

      def tupleLess(attribute: String, value: Any): Seq[(SparkFilterType, String, String, Seq[(Long, Long)])] = {
        Seq((FilterRange, attribute, "", Seq((filters.flatMap {
          case GreaterThan(sf_attribute, sf_value) if sf_attribute == attribute => Seq(sf_value.toString.toLong)
          case GreaterThanOrEqual(sf_attribute, sf_value) if sf_attribute == attribute => Seq(sf_value.toString.toLong)
          case _ => Seq(Long.MinValue)
        }.max, value.toString.toLong))))
      }

      val allFilters: Seq[(SparkFilterType, String, String, Seq[(Long, Long)])] = filters.flatMap {
        case EqualTo(attribute, value) =>
          Seq(
            if (!checkNumeric(attribute))
              (FilterString, attribute, value.toString, Seq((0L, 0L)))
            else
              (FilterLong, attribute, "", Seq((value.toString.toLong, 0L)))
          )

        case GreaterThanOrEqual(attribute, value) if checkNumeric(attribute) =>
          tupleGreater(attribute, value)

        case GreaterThan(attribute, value) if checkNumeric(attribute) =>
          tupleGreater(attribute, value)

        case LessThanOrEqual(attribute, value) if checkNumeric(attribute) =>
          tupleLess(attribute, value)

        case LessThan(attribute, value) if checkNumeric(attribute) =>
          tupleLess(attribute, value)

        case In(attribute, value) =>
          Seq((FilterIn, attribute, value.mkString("'"), Seq((if (checkNumeric(attribute)) 1L else 0L, 0L))))

        case _ => Seq()
      }

      if (allFilters.length > 0) {
        val splitHost = initialHost.split(":")
        val client = new AerospikeClient(null, splitHost(0), splitHost(1).toInt)
        try {
          AerospikeUtils.registerUdfWhenNotExist(client)
        } finally {
          client.close
        }
      }

      val indexedAttrs = allFilters.filter(s => {
        checkIndex(s._2) && s._1.isInstanceOf[AeroFilterType]
      })

      if (filterTypeCache == FilterNone && indexedAttrs.length > 0) {
        //originally declared index query takes priority
        val (filterType: AeroFilterType, filterBin, filterStringVal, filterVals) = indexedAttrs.head
        var tuples: Seq[(Long, Long)] = filterVals
        val lower: Long = filterVals.head._1
        val upper: Long = filterVals.head._2
        val range: Long = upper - lower
        if (partitionsPerServer > 1 && range >= partitionsPerServer) {
          val divided = range / partitionsPerServer
          tuples = (0 until partitionsPerServer)
            .map(i => (lower + divided * i, if (i == partitionsPerServer - 1) upper else lower + divided * (i + 1) - 1))
        }
        new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterType, filterBin, filterStringVal, tuples, allFilters, schemaCache, useUdfWithoutIndexQuery)
      }
      else {
        new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterTypeCache, filterBinCache, filterStringValCache, filterValsCache, allFilters, schemaCache, useUdfWithoutIndexQuery)
      }
    }
    else {
      new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterTypeCache, filterBinCache, filterStringValCache, filterValsCache)
    }
  }

  
}
