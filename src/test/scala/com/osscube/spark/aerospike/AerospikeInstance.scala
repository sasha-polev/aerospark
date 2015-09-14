package com.osscube.spark.aerospike

import com.aerospike.client.AerospikeClient

object AerospikeInstance {

  val host: String = "localhost"
  val port: String = "3000"

  def aerospikeClient: AerospikeClient = {
    new AerospikeClient(null, host, port.toInt)
  }
}
