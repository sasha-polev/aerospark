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

	var config: AerospikeConfig = _
  
  behavior of "QueryEngine from Scala"
  
  it should "Get a client from cache" in {
    config = new AerospikeConfig("127.0.0.1", 3000)
    val client = AerospikeConnection.getClient(config)
  }
  
  it should "Get 3 clients and ensure ther are the same" in {
      val client1 = AerospikeConnection.getClient(config)
      val client2 = AerospikeConnection.getClient(config)
      assert(client1 == client2)
      val client3 = AerospikeConnection.getClient(config)
      assert(client1 == client3)
  }
  
  it should "Get a queryfrom cache" in {
    val qe = AerospikeConnection.getQueryEngine(config)
  }
  
  it should "Insert data" in {
    val cl = AerospikeConnection.getClient(config)
    
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
  }
  
  it should "Select data" in {
    val qe = AerospikeConnection.getQueryEngine(config)
    val stmt = new Statement()
    stmt.setNamespace("test")
    stmt.setSetName("selector")
    var it = qe.select(stmt)
    while (it.hasNext()){
      val keyRecord = it.next()
      val key = keyRecord.key
      val record = keyRecord.record
    }
  }
  
    it should "clean up because it's mother doesn't work here" in {
    val cl = AerospikeConnection.getClient(config)
    
    var i = 0
    for (x <- 1 to 100){
      val name = new Bin("name", "name:" + i);
			val age = new Bin("age", ages(i));
			val colour = new Bin("color", colours(i));
			val animal = new Bin("animal", animals(i));
			val key = new Key("test", "selector", "selector-test:"+ x)
      cl.delete(null, key)
      i += 1
			if ( i == 5)
				i = 0
    }
  }

}