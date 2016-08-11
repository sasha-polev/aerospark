package com.aerospike.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.aerospike.client.AerospikeClient
import org.scalatest.FlatSpec
import com.aerospike.client.Key
import com.aerospike.client.Bin
import com.aerospike.client.query.Statement

class AerospikeConnectionTest extends FlatSpec {
  var conf: SparkConf = _
  var sc:SparkContext = _
  var sqlContext: SQLContext = _
  var config: AerospikeConfig = _
  val seedHost = "52.209.148.20"     // "127.0.0.1"
  val namespace = "mem"              // "test"

  behavior of "AerospikeConnection"
  
  it should " test Client witnout Spark" in {
    config = AerospikeConfig.newConfig(seedHost, 3000, 10000)
    var client = AerospikeConnection.getClient(config)

    for (i <- 1 to 100) {
      val key = new Key(namespace, "rdd-test", "rdd-test-"+i)
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
    var client = AerospikeConnection.getClient(config)

    for (i <- 1 to 100) {
      val key = new Key(namespace, "rdd-test", "rdd-test-"+i)
      client.put(null, key,
         new Bin("one", i),
         new Bin("two", "two:"+i),
         new Bin("three", i.asInstanceOf[Double])
      )
    }
  }
  
  it should " test Client with Spark and Config" in {
    
    config = AerospikeConfig.newConfig(seedHost, 3000, 10000)
    var client = AerospikeConnection.getClient(config)

    for (i <- 1 to 100) {
      val key = new Key(namespace, "rdd-test", "rdd-test-"+i)
      client.put(null, key,
         new Bin("one", i),
         new Bin("two", "two:"+i),
         new Bin("three", i.asInstanceOf[Double])
      )
    }
  }
  it should " test QueryEngine with spark" in {
     var qe = AerospikeConnection.getQueryEngine(config)
     
     var stmt = new Statement()
     stmt.setNamespace(namespace)
     stmt.setSetName("rdd-test")
     val kri = qe.select(stmt)
     var count = 0
     print("\t")
     while (kri.hasNext())
       //println(kri.next())
       print(".")
     qe.close()
    
  }
  it should " do it multiple time" in {
    for( a <- 1 to 10){
     
     val qe = AerospikeConnection.getQueryEngine(config)
     
     var stmt = new Statement()
     stmt.setNamespace(namespace)
     stmt.setSetName("rdd-test")
     val kri = qe.select(stmt)
     var count = 0
     print("\t")
     while (kri.hasNext())
       //println(kri.next())
       print(".")
      qe.close()
    println(s"\tIteration: $a")
    }
    
  }

}