/*
 * Copyright 2014 OSSCube UK.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.osscube.spark.aerospike.rdd


import java.lang

import scala.io.Source
import gnu.crypto.util.Base64

import com.aerospike.client.cluster.Node
import com.aerospike.client.policy.{ClientPolicy, QueryPolicy}
import com.aerospike.client.query.Statement
import com.aerospike.client.{Value, Language, AerospikeClient, Info}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{Row, SQLContext, StructType}

import scala.collection.JavaConverters._

case class AeroRelation(initialHost: String,
                        select: String,
                        partitionsPerServer: Int = 1)(@transient val sqlContext: SQLContext)
  extends PrunedFilteredScan {

  var schemaCache: StructType = null
  var nodeList: Array[Node] = null
  var namespaceCache: String = null
  var setCache: String = null
  var filterTypeCache: Int = 0
  var filterBinCache: String = null
  var filterValsCache: Seq[(Long, Long)] = null
  var filterStringValCache: String = null

  override def schema: StructType = {
    if (schemaCache == null && nodeList == null) {
      val (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal) = AerospikeRDD.parseSelect(select, partitionsPerServer)
      namespaceCache = namespace
      setCache = set
      filterTypeCache = filterType
      filterBinCache = filterBin
      filterValsCache = filterVals
      filterStringValCache = filterStringVal
      val policy = new ClientPolicy()
      val splitHost = initialHost.split(":")
      val client = new AerospikeClient(policy, splitHost(0), splitHost(1).toInt)
      try {
        nodeList = client.getNodes
        val newSt = new Statement()
        newSt.setNamespace(namespace)
        newSt.setSetName(set)
        if (bins.length > 1 || bins(0) != "*")
          newSt.setBinNames(bins: _*)

        val qp: QueryPolicy = new QueryPolicy()
        qp.maxConcurrentNodes = 1
        qp.recordQueueSize = 1
        val recs = client.queryNode(qp, newSt, nodeList(0))
        try {

          recs.next()
          val record = recs.getRecord
          val singleRecBins = record.bins
          var binNames = bins
          if (bins.length == 1 && bins(0) == "*") {
            binNames = singleRecBins.keySet.asScala.toSeq
          }
          schemaCache = StructType(binNames.map { b =>
            singleRecBins.get(b) match {
              case v: Integer => StructField(b, IntegerType, nullable = true)
              case v: lang.Long => StructField(b, LongType, nullable = true)
              case _ => StructField(b, StringType, nullable = true)
            }
          })
        }
        finally {
          recs.close()
        }
      } finally {
        client.close()
      }
    }

    schemaCache
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (schemaCache == null && nodeList == null) {
      val tmp = schema //ensure def schema always called before this method
    }

    //Filter types: 0 none, 1 - equalsString, 2 - equalsLong, 3 - range

    if (filters.length > 0) {
      val index_info = Info.request(nodeList(0), "sindex").split(";")

      var udfFilters : Array[Value] = Array(Value.get(requiredColumns.map(Value.get)))

      def checkIndex(attr: String): Boolean = {
        index_info.filter(_.contains("ns=" + namespaceCache + ":set=" + setCache + ":")).filter(_.contains(":bins=" + attr + ":")).length > 0
      }

      def checkNumeric(attr: String): Boolean = {
        val attrType: String = schemaCache(attr).dataType.typeName
        attrType == "long" || attrType == "integer"
      }

//      def checkDataTypeAndIndex(attr: String): Boolean = {
//        checkNumeric(attr) &&
//          checkIndex(attr)
//      }



      val attrs: Seq[(Int, String, String, Seq[(Long, Long)])] = filters.flatMap {
        case EqualTo(attribute, value)  =>
          Seq(if (!checkNumeric(attribute))
            (1, attribute, value.toString, Seq((0L, 0L)))
          else
            (2, attribute, "", Seq((value.toString.toLong, 0L))))

        case f@GreaterThanOrEqual(_, _) if checkNumeric(f.attribute) =>
          Seq((3, f.attribute, "", Seq((f.value.toString.toLong, filters.flatMap {
            case sf@LessThan(_, _) if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case sf@LessThanOrEqual(_, _) if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case _ => Seq(Long.MaxValue)
          }.min))))


        case f@GreaterThan(_, _) if checkNumeric(f.attribute) =>
          Seq((3, f.attribute, "", Seq((f.value.toString.toLong, filters.flatMap {
            case sf@LessThan(_, _) if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case sf@LessThanOrEqual(_, _) if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case _ => Seq(Long.MaxValue)
          }.min))))


        case f@LessThanOrEqual(_, _) if checkNumeric(f.attribute) =>
          Seq((3, f.attribute, "", Seq((filters.flatMap {
            case sf@GreaterThan(_, _) if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case sf@GreaterThanOrEqual(_, _) if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case _ => Seq(Long.MinValue)
          }.max, f.value.toString.toLong))))

        case f@LessThan(_, _) if checkNumeric(f.attribute) =>
          Seq((3, f.attribute, "", Seq((filters.flatMap {
            case sf@GreaterThan(_, _) if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case sf@GreaterThanOrEqual(_, _) if sf.attribute == f.attribute => Seq(sf.value.toString.toLong)
            case _ => Seq(Long.MinValue)
          }.max,f.value.toString.toLong))))

        case In(attribute, value) =>
          Seq(if (!checkNumeric(attribute))
            (4, attribute, value.mkString("'"), Seq((0L, 0L)))
          else
            (4, attribute, value.mkString("'"), Seq((1L, 0L))))

        case _ => Seq()
      }

      //TODO: remove when not needed
      attrs.foreach {
        case (1, name, stringVal, Seq((_, _))) => udfFilters = udfFilters :+ Value.get(Array(Value.get(1),Value.get(name),Value.get(stringVal)))
        case (2, name, stringVal, Seq((longLower, _))) => udfFilters = udfFilters :+ Value.get(Array(Value.get(2),Value.get(name),Value.get(longLower)))
        case (3, name, stringVal, Seq((longLower, longUpper))) => udfFilters = udfFilters :+ Value.get(Array(Value.get(3),Value.get(name),Value.get(longLower),Value.get(longUpper)))
        case (4, name, stringVal, Seq((longLower, longUpper))) => udfFilters = udfFilters :+ Value.get(Array())
      }

      if(udfFilters.length > 1)
      {
        val udf_name: String = "spark_filters.lua"
        if(!Info.request(nodeList(0), "udf-list").contains("filename=" + udf_name)) { //Should normally happen only once
          val udf_data = Source.fromInputStream(getClass.getResourceAsStream("/" + udf_name)).map(_.toByte).toArray
          val content = Base64.encode(udf_data, 0, udf_data.length, false)
          val sb = new StringBuilder(udf_name.length() + content.length() + 100)
          sb.append("udf-put:filename=")
          .append(udf_name)
          .append(";content=")
          .append(content)
          .append(";content-len=")
          .append(content.length())
          .append(";udf-type=")
          .append(language)
          .append(";")
          val r = new Info(nodeList(0).getConnection(0), sb.toString()) //do not read response for now
        }
      }

      val indexedAttrs = attrs.filter(s => checkIndex(s._2))

      if (filterTypeCache == 0 && indexedAttrs.length > 0) { //originally declared index query takes priority
        val (filterType, filterBin, filterStringVal, filterVals) = indexedAttrs.head
        var tuples: Seq[(Long, Long)] = filterVals
        val lower: Long = filterVals(0)._1
        val upper: Long = filterVals(0)._2
        val range: Long = upper - lower
        if (partitionsPerServer > 1 && range >= partitionsPerServer) {
          val divided = range / partitionsPerServer
          tuples = (0 until partitionsPerServer).map(i => (lower + divided * i, if (i == partitionsPerServer - 1) upper else lower + divided * (i + 1) - 1))
        }
        new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterType, filterBin, filterStringVal, tuples, udfFilters.length > 0,attrs, schemaCache)
      }
      else new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterTypeCache, filterBinCache, filterStringValCache, filterValsCache, filterTypeCache != 0 && udfFilters.length > 0, attrs, schemaCache)
    }
    else new AerospikeRDD(sqlContext.sparkContext, nodeList, namespaceCache, setCache, requiredColumns, filterTypeCache, filterBinCache, filterStringValCache, filterValsCache)

  }
}
