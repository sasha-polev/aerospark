package com.osscube.spark.aerospike.rdd

import java.io.IOException
import java.net.InetAddress
import java.util

import com.aerospike.client.cluster.Node
import com.aerospike.client.{Host, AerospikeClient}
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.query.Statement
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.sql.Row


import scala.collection.JavaConversions._

case class AerospikePartition(index: Int,
                              endpoint: (String, Int, String)) extends Partition

class AerospikePartitioner(val aerospikeHosts: Array[String]) extends HashPartitioner(aerospikeHosts.length) {
  override def equals(other: Any): Boolean = other match {
    case h: AerospikePartitioner => {
      h.aerospikeHosts.diff(aerospikeHosts).length == 0 && aerospikeHosts.diff(h.aerospikeHosts).length == 0
    }
    case _ =>
      false
  }
}

 abstract class BaseAerospikeRDD (
                             @transient sc: SparkContext,
                             @transient val aerospikeHosts: Array[Node],
                             val namespace: String,
                             val set: String,
                             val bins: Array[String],
                             val st: Statement,
                             val makePartitioner: Boolean
                              )
  extends RDD[(String, Row)](sc, Seq.empty) with Logging {

   override protected def getPreferredLocations(split: Partition): Seq[String] = {
     Seq(InetAddress.getByName(split.asInstanceOf[AerospikePartition].endpoint._1).getHostName)
   }

   override val partitioner: Option[Partitioner] = if (makePartitioner) Some(new AerospikePartitioner(aerospikeHosts.map(n => n.getHost.toString))) else None

   override protected def getPartitions: Array[Partition] = {
     (0 until aerospikeHosts.size).map(i => {
       val node: Node = aerospikeHosts(i)
       val aliases = node.getAliases
       if (aliases.length == 0) new AerospikePartition(i, (node.getHost.name, node.getHost.port, node.getName)).asInstanceOf[Partition]
       else {
         val noLocalHost = aliases.filter(h => h.name != "localhost" && h.name != "127.0.0.1")
         val resultName = if (noLocalHost.length != 0) noLocalHost(0) else aliases(0)
         new AerospikePartition(i, (resultName.name, node.getHost.port, node.getName)).asInstanceOf[Partition]
       }
     }).toArray
   }

 }

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def aeroInput(
                 initialHost: (String, Int),
                 namespace: String,
                 set: String = "",
                 bins: Array[String] = Array(""),
                 st: Statement = null,
                 makePartitioner: Boolean = true
                 ) = {
    var hosts : Array[Node] = null
    val policy = new ClientPolicy()
    val client = new AerospikeClient(policy, initialHost._1 , initialHost._2)
    try {
      hosts = client.getNodes()
    }finally {
      client.close()
    }
    new AerospikeRDD(sc, hosts, namespace, set, bins, st, makePartitioner)
  }

}
