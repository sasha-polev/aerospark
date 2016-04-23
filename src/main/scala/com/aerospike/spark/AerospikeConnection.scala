package com.aerospike.spark

import com.aerospike.client.AerospikeClient
/**
 * Cache AerospikeClient instances
 */
object AerospikeConnection {
  val clientCache = new scala.collection.mutable.HashMap[String, AerospikeClient]()
  
  def getClient(host: String, port: Int) : AerospikeClient = {
    getClient(s"$host:$port")
  }
  
  def getClient(hostPort: String) : AerospikeClient = {
    clientCache.getOrElse(hostPort, {
        val splitHost = hostPort.split(":")
        val host = splitHost(0)
        val port = splitHost(1).toInt
        val newClient = new AerospikeClient(host, port)
        val nodes = newClient.getNodes
        for (node <- nodes) {
          clientCache += (node.getHost.toString() -> newClient)
        }
        newClient
      })
    
  }
  
}