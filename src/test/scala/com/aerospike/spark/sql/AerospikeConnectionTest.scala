package com.aerospike.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FlatSpec
import com.aerospike.client.Key
import com.aerospike.client.Bin
import com.aerospike.client.query.Statement

/*
 * TODO: There's nothing tested here.
 */
class AerospikeConnectionTest extends FlatSpec {
  var conf: SparkConf = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _
  val config: AerospikeConfig = AerospikeConfig.newConfig(Globals.seedHost, Globals.port, 10000)

  behavior of "AerospikeConnection"

  it should " test Client without Spark" in {
    val client = AerospikeConnection.getClient(config)

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
    conf = new SparkConf().setMaster("local[*]").setAppName("Aerospike RDD Tests")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
    val client = AerospikeConnection.getClient(config)

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
    val client = AerospikeConnection.getClient(config)

    for (i <- 1 to 100) {
      val key = new Key(Globals.namespace, "rdd-test", "rdd-test-"+i)
      client.put(null, key,
        new Bin("one", i),
        new Bin("two", "two:"+i),
        new Bin("three", i.asInstanceOf[Double])
      )
    }
  }
  it should " test QueryEngine with spark" in {
    val qe = AerospikeConnection.getQueryEngine(config)

    val stmt = new Statement()
    stmt.setNamespace(Globals.namespace)
    stmt.setSetName("rdd-test")
    val kri = qe.select(stmt)
    print("\t")
    while (kri.hasNext)
      print(".")
    qe.close()
  }

  it should " do it multiple time" in {
    for( a <- 1 to 10){
      val qe = AerospikeConnection.getQueryEngine(config)
      val stmt = new Statement()
      stmt.setNamespace(Globals.namespace)
      stmt.setSetName("rdd-test")
      val kri = qe.select(stmt)
      var count = 0
      print("\t")
      while (kri.hasNext)
        //println(kri.next())
        print(".")
      qe.close()
      println(s"\tIteration: $a")
    }
  }
}
