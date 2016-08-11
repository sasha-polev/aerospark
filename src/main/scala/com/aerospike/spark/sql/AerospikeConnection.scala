package com.aerospike.spark.sql

import com.aerospike.helper.query._
import com.aerospike.client.AerospikeClient
import com.aerospike.client.policy.ClientPolicy

/**
 * This class caches the AerospikeClient. The key used to retrive the client is based on the
 * seen host supplied and the port.
 * 
 * The purpose of this class is to eliminate excessive client creation with
 * the goal of having 1 client per executor.
 */
object AerospikeConnection {
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
  def getClient(config: AerospikeConfig) : AerospikeClient = synchronized{
    val host = config.get(AerospikeConfig.SeedHost);
    val port = config.get(AerospikeConfig.Port);
    var client = clientCache.getOrElse(s"$host:$port", {
        newClient(config)
      })
    if (!client.isConnected())
      client = newClient(config)
    client   
  }
  
  private def newClient(config: AerospikeConfig): AerospikeClient = {
   
        val host = config.get(AerospikeConfig.SeedHost).toString();
        val portProperty = config.get(AerospikeConfig.Port);
        val port = portProperty match {
          case _:Int => portProperty.asInstanceOf[Int]
          case _:String => portProperty.asInstanceOf[String].toInt
          case None => 3000
        }
        val timeoutProperty = config.get(AerospikeConfig.TimeOut) 
        val timeOut:Int = timeoutProperty match {
          case _:Int => timeoutProperty.asInstanceOf[Int]
          case _:String => timeoutProperty.asInstanceOf[String].toInt
          case None => 1000
        }
        val clientPolicy = new ClientPolicy
        clientPolicy.timeout = timeOut
        clientPolicy.failIfNotConnected = true
        val newClient = new AerospikeClient(clientPolicy, host, port)
    
        // set all the timeouts
        newClient.writePolicyDefault.timeout = timeOut
        newClient.readPolicyDefault.timeout = timeOut
        newClient.scanPolicyDefault.timeout = timeOut
        newClient.queryPolicyDefault.timeout = timeOut
        
        val nodes = newClient.getNodes
        for (node <- nodes) {
          clientCache += (node.getHost.toString() -> newClient)
        }
        newClient
  }
  
}