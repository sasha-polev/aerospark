package com.osscube.spark.aerospike.rdd


import com.aerospike.client.cluster.Node
import com.aerospike.client.{Record, Host, AerospikeClient}
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.query.{Filter, RecordSet, Statement}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._


class AerospikeRDD(
                    @transient sc: SparkContext,
                    @transient aerospikeHosts: Array[Node],
                    val namespace: String,
                    val set: String,
                    val bins: Array[String],
                    val filter: String, // concept of bin = string or bin = int or bin BETWEEN 20 and 29
                    @transient makePartitioner: Boolean
                    ) extends BaseAerospikeRDD (sc, aerospikeHosts,  makePartitioner) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(String, Row)]  = {
    val newSt = new Statement()
    newSt.setNamespace(namespace)
    newSt.setSetName(set)
    newSt.setBinNames(bins:_*)
    val aeroFilter: Filter = createFilter
    if(aeroFilter != null)
      newSt.setFilters(aeroFilter)
    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    val endpoint = partition.endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint._1)
    val policy = new ClientPolicy()
    var res: RecordSet = null
    try {
      val client = new AerospikeClient(policy, endpoint._1, endpoint._2)
      res = client.queryNode(policy.queryPolicyDefault, newSt, client.getNode(endpoint._3))
      new RecordSetIteratorWrapper(res).asScala.map { p =>
        val key = p._1.userKey.toString
        val binValues = p._2.bins.values
        val r = Row.fromSeq(binValues.toArray)
        (key, r)
      }
    }
    finally
    {
      res.close()
    }
  }

  protected def createFilter: Filter = {
     if (filter != null && !filter.isEmpty) {
      val upperFilter: String = filter.toUpperCase
      if (upperFilter.contains("BETWEEN")) {
        val length: Int = "BETWEEN".length
        val ind = upperFilter.indexOf("BETWEEN") + length
        val bounds = upperFilter.substring(ind).split(" AND ")
        if (bounds.length == 2)
          try {
            Filter.range(filter.substring(0, length - 1).trim, bounds(0).trim.toLong, bounds(0).trim.toLong)
          }
          catch {
            case nfe: NumberFormatException => logError("Could not parse number: " + filter + ", " + nfe); null
            case e: Exception => logError("Filter creation error: " + filter + ", " + e); null
          }
        else {
          logError("Wrong filter for between: " + filter)
          null
        }
      }
      else if (filter.contains("=")) {
        val parts = filter.split("=").map(_.trim)
        if (parts.length == 2) {
          if (parts(1).forall(_.isDigit))
            Filter.equal(parts(0), parts(1).toLong)
          else
            Filter.equal(parts(0), parts(1))
        }
        else
          null
      }
      else {
        logError("Could not understand filter: " + filter)
        null
      }
    }
    else
      null
  }
}