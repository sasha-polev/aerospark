package com.aerospike.spark.sql

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.helper.query.QueryEngine
import org.scalatest.{FlatSpec, Matchers}

class AerospikeRDDTest extends FlatSpec with Matchers {
  var client: AerospikeClient = _
  var queryEngine: QueryEngine = _

  behavior of "AerospikeRDD"
  
  it should " create data" in {
    client = AerospikeConnection.getClient("localhost", 3000)
    queryEngine = new QueryEngine(client)



    for (i <- 1 to 100) {
      val key = new Key("test", "rdd-test", "rdd-test-"+i)
      client.put(null, key,
         new Bin("one", i),
         new Bin("two", "two:"+i),
         new Bin("three", i.asInstanceOf[Double])
      )
    }
    
    queryEngine.close()
    client.close()
  }
}
