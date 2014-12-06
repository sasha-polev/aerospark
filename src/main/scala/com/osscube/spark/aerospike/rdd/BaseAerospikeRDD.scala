package com.osscube.spark.aerospike.rdd


import java.lang
import java.net.InetAddress


import com.aerospike.client.cluster.Node
import com.aerospike.client.{Host, AerospikeClient}
import com.aerospike.client.policy.{QueryPolicy, ClientPolicy}
import com.aerospike.client.query.Statement
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.catalyst.types.LongType
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.types.StructType
import scala.collection.JavaConverters._





//Filter types: 0 none, 1 - equalsString, 2 - equalsLong, 3 - range

case class AerospikePartition(index: Int,
                              endpoint: (String, Int, String),
                              startRange:Long,
                              endRange:Long
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

 abstract class BaseAerospikeRDD (
                             @transient sc: SparkContext,
                             @transient val aerospikeHosts: Array[Node],
                             val filterVals :  Seq[(Long, Long)]
                                    )
  extends RDD[Row](sc, Seq.empty) with Logging {

   override protected def getPreferredLocations(split: Partition): Seq[String] = {
     Seq(InetAddress.getByName(split.asInstanceOf[AerospikePartition].endpoint._1).getHostName)
   }

//   override val partitioner: Option[Partitioner] = if (makePartitioner) Some(new AerospikePartitioner(aerospikeHosts.map(n => n.getHost.toString))) else None

   override protected def getPartitions: Array[Partition] = {
     {for {i <- (0 until aerospikeHosts.size)
           node: Node = aerospikeHosts(i)
           aliases = node.getAliases
           (j, k) <- filterVals.zipWithIndex
           resultName  = if (aliases.length == 0) node.getHost.name else {
             val noLocalHost = aliases.filter(h => h.name != "localhost" && h.name != "127.0.0.1")
             if (noLocalHost.length != 0) noLocalHost(0).name else aliases(0).name
           }
           part = new AerospikePartition(i * filterVals.length + k, (resultName, node.getHost.port, node.getName), j._1, j._2).asInstanceOf[Partition]
      } yield part}.toArray
   }
 }



class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def aeroInput(
    initialHost: (String, Int),
    select: String,
    numPartitionsPerServerForRange : Int = 1

  ) = {
    val (namespace, set, bins, filterType, filterBin, filterVals , filterStringVal) = AerospikeRDD.parseSelect(select, numPartitionsPerServerForRange)
    var hosts : Array[Node] = null
    val policy = new ClientPolicy()
    val client = new AerospikeClient(policy, initialHost._1 , initialHost._2)

    try {
      hosts = client.getNodes()
    }finally {
      client.close()
    }
    new AerospikeRDD(sc, hosts, namespace, set, bins, filterType, filterBin, filterStringVal, filterVals)
  }

  def aeroSInput(
                 initialHost: (String, Int),
                 select: String,
                 cont: SQLContext,
                 numPartitionsPerServerForRange : Int = 1
                 ) = {
    val (namespace, set, bins, filterType, filterBin, filterVals , filterStringVal) = AerospikeRDD.parseSelect(select, numPartitionsPerServerForRange)
    var nodes : Array[Node] = null
    val policy = new ClientPolicy()
    val client = new AerospikeClient(policy, initialHost._1 , initialHost._2)
    var struct : StructType = null
    try {
    nodes = client.getNodes()
    val newSt = new Statement()
    newSt.setNamespace(namespace)
    newSt.setSetName(set)
    newSt.setBinNames(bins: _*)

    val qp: QueryPolicy = new QueryPolicy()
    qp.maxConcurrentNodes = 1
    qp.recordQueueSize = 1
    val recs = client.queryNode(qp, newSt, nodes(0))
    recs.next()
    val record = recs.getRecord
    val singleRecBins = record.bins
    var binNames = bins
    if(bins.length == 1 && bins(0) == "*")
      binNames = singleRecBins.keySet.asScala.toSeq
    struct = StructType(binNames.map { b =>
      singleRecBins.get(b) match {
        case v: Integer => StructField(b, IntegerType, true)
        case v: lang.Long => StructField(b, LongType, true)
        case _ => StructField(b, StringType, true)
      }
    })
    recs.close()
    }finally {
      client.close()
    }
    cont.applySchema(new AerospikeRDD(sc, nodes, namespace, set, bins, filterType, filterBin, filterStringVal, filterVals), struct)
  }







}

