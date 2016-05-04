package com.aerospike.spark.sql

import org.scalatest.FlatSpec

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.query.Statement
import com.aerospike.helper.query.KeyQualifier
import com.aerospike.helper.query.QueryEngine
import com.aerospike.client.Value
import com.aerospike.client.Key


class QueyEngineTest extends FlatSpec{
  
  val ages = Array(25,26,27,28,29)
  val colours = Array("blue","red","yellow","green","orange")
	val animals = Array("cat","dog","mouse","snake","lion")

  
  behavior of "QueryEngine from Scala"
  
  it should "Create from Client" in {
    val client = new AerospikeClient("127.0.0.1", 3000)
    val qe = new QueryEngine(client)
  }
  
  it should "Get from cache" in {
    val config = new AerospikeConfig("127.0.0.1", 3000)
    val qe = AerospikeConnection.getQueryEngine(config)
  }
  
  it should "Insert and Select data" in {
    val config = new AerospikeConfig("127.0.0.1", 3000)
    val qe = AerospikeConnection.getQueryEngine(config)
    val cl = AerospikeConnection.getClient(config)
    
    val stmt = new Statement()
    stmt.setNamespace("test")
    stmt.setSetName("selector")
    var i = 0
    for (x <- 1 to 100){
      val name = new Bin("name", "name:" + i);
			val age = new Bin("age", ages(i));
			val colour = new Bin("color", colours(i));
			val animal = new Bin("animal", animals(i));
			val key = new Key("test", "selector", "selector-test:"+ x)
      cl.put(null, key, name, age, colour, animal)
      i += 1
			if ( i == 5)
				i = 0
    }
    
    var it = qe.select(stmt)
    while (it.hasNext())
      println(it.next())
  }
}