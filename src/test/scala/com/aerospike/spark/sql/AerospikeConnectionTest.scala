package com.aerospike.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FlatSpec

import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.query.Statement
import org.scalatest.Matchers
import com.aerospike.client.AerospikeClient
import com.aerospike.spark.SparkASITSpecBase
import com.aerospike.spark.Globals

/*
 * TODO: There's nothing tested here.
 */
class AerospikeConnectionTest extends FlatSpec with Matchers with SparkASITSpecBase{
//  val config: AerospikeConfig = AerospikeConfig(conf)

  behavior of "AerospikeConnection"

  it should "Get a client from cache" in {
    val client = AerospikeConnection.getClient(conf)
    client shouldBe a [AerospikeClient]
  }

  it should "Get 3 clients and ensure they are the same" in {
    val client1 = AerospikeConnection.getClient(conf)
    val client2 = AerospikeConnection.getClient(conf)
    assert(client1 == client2)
    val client3 = AerospikeConnection.getClient(conf)
    assert(client1 == client3)
  }


  it should " test Client without Spark" in {
    val client = AerospikeConnection.getClient(conf)

    for (i <- 1 to 100) {
      val key = new Key(Globals.namespace, "rdd-test", "rdd-test-"+i)
      client.put(null, key,
        new Bin("one", i),
        new Bin("two", "two:"+i),
        new Bin("three", i.asInstanceOf[Double])
      )
      client.get(null, key)
      client.delete(null, key)
    }
  }

  it should " test Client with Spark" in {
    conf.setMaster("local[*]").setAppName("Aerospike RDD Tests")
    val client = AerospikeConnection.getClient(conf)

    for (i <- 1 to 100) {
      val key = new Key(Globals.namespace, "rdd-test", "rdd-test-"+i)
      client.put(null, key,
        new Bin("one", i),
        new Bin("two", "two:"+i),
        new Bin("three", i.asInstanceOf[Double])
      )
    }
  }

  it should " test Client with Spark and Config" in {
    val client = AerospikeConnection.getClient(conf)

    for (i <- 1 to 100) {
      val key = new Key(Globals.namespace, "rdd-test", "rdd-test-"+i)
      client.put(null, key,
        new Bin("one", i),
        new Bin("two", "two:"+i),
        new Bin("three", i.asInstanceOf[Double])
      )
    }
  }


  it should " do it multiple time" in {
    val config: AerospikeConfig = AerospikeConfig(conf)
    for( a <- 1 to 10){
      val qe = AerospikeConnection.getQueryEngine(config)
      val stmt = new Statement()
      stmt.setNamespace(Globals.namespace)
      stmt.setSetName("rdd-test")
      val kri = qe.select(stmt)
      var count = 0
      print("\t")
      while (kri.hasNext)
        println(kri.next())
        print(".")
      qe.close()
      println(s"\tIteration: $a")
    }
  }
}
