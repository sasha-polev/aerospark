package com.aerospike.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.Filter
import com.aerospike.helper.query._
import com.aerospike.client.query.Statement
import scala.collection.JavaConverters._
import com.aerospike.client.Host
import scala.collection.mutable.ArrayBuffer
import java.net.InetAddress
import com.aerospike.client.cluster.Node


case class AerospikePartition(index: Int,
                              node: Node
                               ) extends Partition()


class AerospikeRDD(
                    @transient val sc: SparkContext,
                    @transient val aerospikeConfig: AerospikeConfig,
                    val bins: Seq[String] = null,
                    val qualifiers: Array[Qualifier] = null
                    ) extends RDD[Row](sc, Seq.empty) with Logging {  


  override protected def getPartitions: Array[Partition] = {
    {
      
      var client = AerospikeConnection.getClient(aerospikeConfig) 
      var nodes = client.getNodes
      for {i <- 0 until nodes.size
           node: Node = nodes(i)
           part = new AerospikePartition(i , node).asInstanceOf[Partition]
      } yield part
    }.toArray
  }
  
  
  def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    logDebug("Starting compute() for partition: " + partition.index)
    val stmt = new Statement()
    stmt.setNamespace(aerospikeConfig.namespace())
    stmt.setSetName(aerospikeConfig.set())
    stmt.setBinNames(bins: _*)
    
    val queryEngine = AerospikeConnection.getQueryEngine(aerospikeConfig)
    
    val kri = queryEngine.select(stmt, false, partition.node, qualifiers: _*)
    val res = kri.asScala
    
    context.addTaskCompletionListener(context => { kri.close() })
    
    res.map(kr => {
      var fields = scala.collection.mutable.MutableList(
          kr.key.namespace,
          kr.key.setName,
          kr.key.userKey,
          kr.key.digest,
          kr.record.expiration,
          kr.record.generation
          )
      bins.foreach { bin => fields += kr.record.bins.get(bin) }    
      Row.fromSeq(fields.toSeq)
    })

  }
  
}


