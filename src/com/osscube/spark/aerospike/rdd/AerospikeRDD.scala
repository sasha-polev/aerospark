package com.osscube.spark.aerospike.rdd


import com.aerospike.client.cluster.Node
import com.aerospike.client.{Host, AerospikeClient}
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.query.Statement
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.sql.Row
import scala.collection.JavaConversions._


class AerospikeRDD(
                    @transient sc: SparkContext,
                    @transient aerospikeHosts: Array[Node],
                    @transient namespace: String,
                    @transient set: String,
                    @transient bins: Array[String],
                    @transient st: Statement,
                    @transient makePartitioner: Boolean
                    ) extends BaseAerospikeRDD (sc, aerospikeHosts, namespace, set, bins, st, makePartitioner) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(String, Row)]  = {
    var newSt : Statement = null
    if(st == null) {
      newSt = new Statement()
      newSt.setNamespace(namespace)
      newSt.setSetName(set)
      newSt.setBinNames(bins:_*)
    }
    else
      newSt = st
    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    val endpoint = partition.endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint._1)
    val policy = new ClientPolicy()
    val client = new AerospikeClient(policy, endpoint._1 , endpoint._2)
    val res = client.queryNode(policy.queryPolicyDefault, newSt, client.getNode(endpoint._3))
    new RecordSetIteratorWrapper(res).asInstanceOf[Iterator[(String, Row)]]
  }

}