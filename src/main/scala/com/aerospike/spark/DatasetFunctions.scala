/*
 * Copyright 2012-2017 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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
package com.aerospike.spark

import scala.collection.JavaConversions._

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Key
import com.aerospike.client.Record
import com.aerospike.client.Value
import com.aerospike.spark.sql.AerospikeConfig
import com.aerospike.spark.sql.AerospikeConnection
import java.util.HashMap
import java.lang.reflect.Method


/**
 * 
 * Provides Aerospike-specific methods on [[org.apache.spark.sql.Dataset Dataset]]
 * 
 *  @author Michael Zhang
 */
final class DatasetFunctions[T](dataset: Dataset[T]) extends Serializable {

  val spark: SparkSession = dataset.sparkSession

  /**
   * Utilized Aerospike batch read for dataset join
   */
  def batchJoin(keyCol: String, set: String)(
  implicit client: AerospikeClient = AerospikeConnection.getClient(spark.sparkContext.getConf)): Map[Any, Record] = {
    val kVal = dataset.select(keyCol).collect
    val ks = for (ak <- kVal) yield new Key(spark.sparkContext.getConf.get(AerospikeConfig.NameSpace), set, Value.get(ak.get(0)))

    (kVal zip client.get(null, ks)).toMap[Any, Record]
  }
  
  def aeroJoin(keyCol: String, set: String): Iterable[Row] = {
    val rs = batchJoin(keyCol, set)
    for {
        (k,v) <- batchJoin(keyCol, set) 
        if(Option(v).isDefined)
    } yield Row (k.asInstanceOf[Row].get(0), mapAsScalaMap(v.bins))
  }

  def aeroIntersect(keyCol: String, set: String): Dataset[T] = {
    val rs = batchJoin(keyCol, set)
    val binMap = for((key, record) <- rs) yield (key.asInstanceOf[Row].get(0) -> record.bins)
    dataset.filter(data => dataMatch(keyCol, data, binMap))
  }

  def saveToAerospike(set: String, keyBin: String): Unit = {
    val conf = spark.sparkContext.getConf

    dataset.write
      .mode(SaveMode.valueOf(conf.get(AerospikeConfig.SaveMode, "Ignore")))
      .format("com.aerospike.spark.sql")
      .option(AerospikeConfig.SetName, set)
      .option(AerospikeConfig.UpdateByKey, keyBin)
      .save()
  }

  implicit def reflector(ref: T) = new {
    def getV(name: String): Any = {
      ref.getClass.getMethods.find(_.getName == name).getOrElse(None) match{
        case method:Method =>method.invoke(ref)
        case _ => None
      }
    }

    def setV(name: String, value: Any): Unit = ref.getClass.getMethods.find(_.getName == name + "_$eq").getOrElse(return).invoke(ref, value.asInstanceOf[AnyRef])
  }

  private def dataMatch(keyField: String, data: T, binMap: Map[Any, java.util.Map[String, Object]]): Boolean = {
    val bins = binMap.get(data.getV(keyField)).getOrElse(new HashMap[String, Object]())
    var matched = true
    bins.foreach(kv => if (data.getV(kv._1.asInstanceOf[String]) != kv._2) matched = false)
    matched
  }

}