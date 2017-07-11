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

import java.lang.reflect.Method
import java.util.HashMap

import scala.collection.JavaConversions.mapAsScalaMap
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Key
import com.aerospike.client.Record
import com.aerospike.client.Value
import com.aerospike.spark.sql.AerospikeConfig
import com.aerospike.spark.sql.AerospikeConnection
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Try

/**
 * 
 * Provides Aerospike-specific functions on [[org.apache.spark.sql.Dataset Dataset]]
 * 
 *  @author Michael Zhang
 */
final class AeroSparkDatasetFunctions[T](dataset: Dataset[T]) extends Serializable {

  val spark: SparkSession = dataset.sparkSession
  val sparkConf = spark.sparkContext.getConf
    
  def aeroWrite(): DataFrameWriter[T]={
    dataset.write.aerospike
  }

  /**
   * Perform Dataset join with aerospike set
   * @param keyCol:	key column of the input dataset
   * @param set:		set name of aerospike
   * 
   * @return a Map[Any, Map[String, Object]]: a map with key to the map of bin name to value
   */
  def aeroJoinMap(keyCol: String, set: String, bins: Array[String] = Array.empty[String], partition: Iterator[T] = Iterator[T]()): Map[Any, Map[String, Object]] = {
    for {
        (k,v) <- batchJoin(keyCol, set, bins, partition) 
        if(Option(v).isDefined)
    } yield (k, v.bins.toMap)
  }
      
  /**
   * Perform Dataset join with aerospike set
   * @param keyCol:	key column of the input dataset
   * @param set:		set name of aerospike
   * 
   * @return Dataset[A]
   */
  def aeroJoin[A: TypeTag: ClassTag](keyCol: String, set: String)(implicit ev: A <:< AeroKV): Dataset[A] = {
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[A]
    dataset.mapPartitions{iter =>
      val rs = for {
          (k,v) <- aeroJoinMap(keyCol, set, classAccessors[A].map(m => m.name.toString).toArray, iter)
          val v2 = fromMap[A](v.+ ("__key"-> k))
      } yield (v2.asInstanceOf[A])
      rs.toIterator
    }
  }
   
  /**
   * Perform Dataset intersect with aerospike set
   * @param keyCol:	key column of the input dataset
   * @param set:		set name of aerospike
   * 
   * @return a Dataset[T]
   */
  def aeroIntersect(keyCol: String, set: String): Dataset[T] = {
    val binMap = for {
        (k,v) <- batchJoin(keyCol, set) 
        if(Option(v).isDefined)
    } yield (k.asInstanceOf[Key].userKey.toString()-> v.bins)
    dataset.filter(data => dataMatch(keyCol, data, binMap))
  }

  def save(set: String, keyBin: String): Unit = {
    val conf = spark.sparkContext.getConf

    dataset.write.aerospike(set, keyBin)
      .mode(SaveMode.valueOf(conf.get(AerospikeConfig.SaveMode, "Ignore")))
      .save()
  }
  
  def saveToAerospike(keyBin: String)(implicit m: reflect.Manifest[T]): Unit = {
    save(m.runtimeClass.getSimpleName, keyBin)
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

  private def dataMatch(keyField: String, data: T, binMap: Map[String, java.util.Map[String, Object]]): Boolean = {
    val bins = binMap.get(data.getV(keyField).asInstanceOf[String]).getOrElse(new HashMap[String, Object]())
    var matched = true
    bins.foreach(kv => if (data.getV(kv._1) != kv._2) matched = false)
    matched
  }
  
    /**
   * Utilized Aerospike batch read for dataset join
   * @param keyCol:	key column of the input dataset
   * @param set:		set name of aerospike
   * 
   * @return a map of key->record
   */
  private def batchJoin(keyCol: String, set: String, binNames:Array[String] = Array[String](), partition: Iterator[T] = Iterator[T]()): Map[Any, Record] = {
    val kVal = if(partition.isEmpty) dataset.select(keyCol).collect().map(_.get(0)) else partition.map(_.getV(keyCol)).toArray
    val batchMax:Int = AerospikeConfig(sparkConf).get(AerospikeConfig.BatchMax).toString.toInt
    
    val futureJobs = for{ 
        g <- kVal.grouped(batchMax)
        val ks = for (ak <- g) yield new Key(sparkConf.get(AerospikeConfig.NameSpace), set, Value.get(ak))
    } yield batchRead(ks, binNames)  
    
    val f = Future.collect(futureJobs.toList)
    Await.result(f) reduce(_++_)
  }
  
  private def batchRead(ks: Array[Key], binNames: Array[String]): Future[Map[Any, Record]]={
    val returnVal = new Promise[Map[Any, Record]]
    val client: AerospikeClient = AerospikeConnection.getClient(sparkConf)
    new Promise(Try(if (binNames.isEmpty) client.get(null, ks) else  client.get(null, ks, binNames:_*))) onSuccess{ records =>
      returnVal.setValue((ks zip records).toMap[Any, Record])
    } onFailure { exc => // callback for failed batch fetch
        returnVal.setException(exc)
    }
    returnVal
  }
  
  private def classAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList

}

trait AeroKV {def __key: Any}
