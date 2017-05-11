package com.aerospike.spark.sql


import java.net.InetSocketAddress;
import org.apache.spark.SparkConf

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Info
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.helper.query._
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.aerospike.client.cluster.Connection
import org.apache.spark.sql.RuntimeConfig

/**
  * This class caches the AerospikeClient. The key used to retrive the client is based on the
  * seen host supplied and the port.
  *
  * The purpose of this class is to eliminate excessive client creation with
  * the goal of having 1 client per executor.
  */
object AerospikeConnection extends LazyLogging {
  val clientCache = new scala.collection.mutable.HashMap[String, AerospikeClient]()
  val queryEngineCache = new scala.collection.mutable.HashMap[AerospikeClient, QueryEngine]()

  def getQueryEngine(config: AerospikeConfig) : QueryEngine = synchronized {
    val client = getClient(config: AerospikeConfig)
    queryEngineCache.getOrElse(client, {
      val newEngine = new QueryEngine(client)
      newEngine.refreshCluster()
      queryEngineCache += (client -> newEngine)
      newEngine
    })
  }
   
  def getClient(config: SparkConf) : AerospikeClient = synchronized{
     getClient(AerospikeConfig(config))
  }
  
  def getClient(config: RuntimeConfig) : AerospikeClient = synchronized{
     getClient(AerospikeConfig(config))
  }
 
  def getClient(config: AerospikeConfig) : AerospikeClient = synchronized{
    val host = config.get(AerospikeConfig.SeedHost)
    val port = config.get(AerospikeConfig.Port)
    var client = clientCache.getOrElse(s"$host $port", newClient(config))
    if (!client.isConnected){
      client = newClient(config)
    }
    client
  }

  private def newClient(config: AerospikeConfig): AerospikeClient = {

    val host = config.get(AerospikeConfig.SeedHost).toString
    val port = config.get(AerospikeConfig.Port) match {
      case i: Int => i
      case s: String => s.toInt
      case None => 3000
    }
    val timeOut:Int = config.get(AerospikeConfig.TimeOut) match {
      case i: Int => i
      case s: String => s.toInt
      case None => 1000
    }
    val socketTimeOut:Int = config.get(AerospikeConfig.SocketTimeOut) match {
      case i: Int => i
      case s: String => s.toInt
      case None => 1000
    }

    val clientPolicy = new ClientPolicy
    clientPolicy.timeout = timeOut
    clientPolicy.failIfNotConnected = true
    clientPolicy.maxConnsPerNode
    val newClient = new AerospikeClient(clientPolicy, host, port)

    // set all the timeouts
    newClient.writePolicyDefault.timeout = timeOut
    newClient.readPolicyDefault.timeout = timeOut
    newClient.scanPolicyDefault.timeout = timeOut
    newClient.scanPolicyDefault.socketTimeout = socketTimeOut
    newClient.queryPolicyDefault.timeout = timeOut
 
    for (node <- newClient.getNodes) {
      clientCache += (node.getHost.toString -> newClient)
    }
    newClient
  }
}
