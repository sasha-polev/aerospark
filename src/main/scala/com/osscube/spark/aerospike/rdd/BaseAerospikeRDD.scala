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


import java.net.InetAddress

import com.aerospike.client.AerospikeClient
import com.aerospike.client.cluster.Node
import com.aerospike.client.policy.ClientPolicy
import com.osscube.spark.aerospike.AqlParser
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.Filter


//Filter types: 0 none, 1 - equalsString, 2 - equalsLong, 3 - range

case class AerospikePartition(index: Int,
                              endpoint: (String, Int, String),
                              startRange: Long,
                              endRange: Long
                               ) extends Partition()


//class AerospikePartitioner(val aerospikeHosts: Array[String]) extends HashPartitioner(aerospikeHosts.length) {
//  override def equals(other: Any): Boolean = other match {
//    case h: AerospikePartitioner => {
//      h.aerospikeHosts.diff(aerospikeHosts).length == 0 && aerospikeHosts.diff(h.aerospikeHosts).length == 0
//    }
//    case _ =>
//      false
//  }
//}

abstract class BaseAerospikeRDD(
                                 @transient val sc: SparkContext,
                                 @transient val aerospikeHosts: Array[Node],
                                 val filterVals: Seq[(Long, Long)]
                                 )
  extends RDD[Row](sc, Seq.empty) with Logging {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(InetAddress.getByName(split.asInstanceOf[AerospikePartition].endpoint._1).getHostName)
  }

  //   override val partitioner: Option[Partitioner] = if (makePartitioner) Some(new AerospikePartitioner(aerospikeHosts.map(n => n.getHost.toString))) else None

  override protected def getPartitions: Array[Partition] = {
    {
      for {i <- 0 until aerospikeHosts.size
           node: Node = aerospikeHosts(i)
           aliases = node.getAliases
           (j, k) <- filterVals.zipWithIndex
           resultName = if (aliases.length == 0) node.getHost.name
           else {
             val noLocalHost = aliases.filter(h => h.name != "localhost" && h.name != "127.0.0.1")
             if (noLocalHost.length != 0) noLocalHost(0).name else aliases(0).name
           }
           part = new AerospikePartition(i * filterVals.length + k, (resultName, node.getHost.port, node.getName), j._1, j._2).asInstanceOf[Partition]
      } yield part
    }.toArray
  }
}


class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def aeroInput(
                 initialHost: String,
                 select: String,
                 partitionsPerServer: Int = 1,
 		 timeout: Int = 1000
                 ) = {
    val (namespace, set, bins, filterType, filterBin, filterVals, filterStringVal) = AqlParser.parseSelect(select, partitionsPerServer).toArray()
    var hosts: Array[Node] = null
    val policy = new ClientPolicy()
    val splitHost = initialHost.split(":")
    policy.timeout=timeout
    val client = new AerospikeClient(policy, splitHost(0), splitHost(1).toInt)

    try {
      hosts = client.getNodes
    } finally {
      client.close()
    }
    new AerospikeRDD(sc, hosts, namespace, set, bins, filterType, filterBin, filterStringVal, filterVals)
  }

  def aeroSInput(
                  initialHost: String,
                  select: String,
                  cont: SQLContext,
                  partitionsPerServer: Int = 1
                  ) = {

    val rel = AeroRelation(initialHost, select, partitionsPerServer)(cont)
    val schema = rel.schema
    cont.applySchema(rel.buildScan(schema.fieldNames.toArray, Array[Filter]()), schema)
  }


}

