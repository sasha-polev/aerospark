package com.aerospike.spark.sql

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.helper.query.QueryEngine
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class AerospikeRelationTest extends FlatSpec {
  var client: AerospikeClient = _
  var queryEngine: QueryEngine = _

  behavior of "Aerospike Relation"
  
  it should " create data" in {
    client = AerospikeConnection.getClient("localhost", 3000)

    for (i <- 1 to 100) {
      val key = new Key("test", "rdd-test", "rdd-test-"+i)
      client.put(null, key,
         new Bin("one", i),
         new Bin("two", "two:"+i),
         new Bin("three", i.asInstanceOf[Double])
      )
    }
    
    client.close()

    
    val conf = new SparkConf().setMaster("local[*]").setAppName("Aerospike RDD Tests")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    println("Reading in data frame")
		var thingsDF = sqlContext.read.
						format("com.aerospike.spark.sql").
						option("aerospike.seedhost", "127.0.0.1").
						option("aerospike.port", "3000").
						option("aerospike.namespace", "test").
						load 
		thingsDF.printSchema()
    
  }
}
