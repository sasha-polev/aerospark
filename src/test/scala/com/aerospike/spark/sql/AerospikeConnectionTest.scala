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

  behavior of "AerospikeConnection"
  
  it should " test Client witnout Spark" in {
    var client = AerospikeConnection.getClient("localhost", 3000)

    for (i <- 1 to 100) {
      val key = new Key("test", "rdd-test", "rdd-test-"+i)
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
    var client = AerospikeConnection.getClient("localhost", 3000)

    for (i <- 1 to 100) {
      val key = new Key("test", "rdd-test", "rdd-test-"+i)
      client.put(null, key,
         new Bin("one", i),
         new Bin("two", "two:"+i),
         new Bin("three", i.asInstanceOf[Double])
      )
    }
  }
  
  it should " test Client with Spark and Config" in {
    
    config = AerospikeConfig.newConfig("localhost", 3000)
    var client = AerospikeConnection.getClient(config)

    for (i <- 1 to 100) {
      val key = new Key("test", "rdd-test", "rdd-test-"+i)
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
     stmt.setNamespace("test")
     stmt.setSetName("rdd-test")
     val kri = qe.select(stmt)
     var count = 0
     while (kri.hasNext())
       println(kri.next())
       
    
  }
  it should " do it a 2nd time" in {
     var qe = AerospikeConnection.getQueryEngine(config)
     
     var stmt = new Statement()
     stmt.setNamespace("test")
     stmt.setSetName("rdd-test")
     val kri = qe.select(stmt)
     var count = 0
     while (kri.hasNext())
       println(kri.next())
       
    
  }

}