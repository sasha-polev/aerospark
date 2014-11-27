package com.osscube.spark.aerospike.rdd


import com.aerospike.client.cluster.Node
import com.aerospike.client.{Host, AerospikeClient}
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.query.Statement
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.sql.Row
/**
 * Created by Sasha on 27/11/2014.
 */
class AerospikeRDD(
                    @transient sc: SparkContext,
                    @transient aerospikeHosts: Array[Node],
                    @transient namespace: String,
                    @transient set: String,
                    @transient bins: Set[String],
                    @transient st: Statement,
                    @transient makePartitioner: Boolean
                    ) extends BaseAerospikeRDD (sc, aerospikeHosts, namespace, set, bins, st, makePartitioner) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(String, Row)]  = {
    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    val endpoint = partition.endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint._1)
    val policy = new ClientPolicy()
    val client = new AerospikeClient(policy, endpoint._1 , endpoint._2)
    client.scanNode(policy.scanPolicyDefault, endpoint._3, namespace, set, null, bins.toArray:_*)
  }

}