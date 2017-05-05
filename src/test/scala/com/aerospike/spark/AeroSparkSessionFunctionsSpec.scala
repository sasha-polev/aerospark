package com.aerospike.spark

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.policy.WritePolicy
import com.aerospike.spark.sql.AerospikeConnection

class AeroSparkSessionFunctionsSpec extends FlatSpec with Matchers with SparkASITSpecBase {
  val ages = Array(25, 26, 27, 28, 29)
  val colours = Array("blue", "red", "yellow", "green", "orange")
  val animals = Array("cat", "dog", "mouse", "snake", "lion")
  val wp = new WritePolicy()
  wp.expiration = 600

  behavior of " Session Func for spark"

  it should "Insert data" in {
    val cl = AerospikeConnection.getClient(conf)

    var i = 0
    for (x <- 1 to 100) {
      val name = new Bin("name", "name:" + i)
      val age = new Bin("age", ages(i))
      val colour = new Bin("color", colours(i))
      val animal = new Bin("animal", animals(i))
      val key = new Key(Globals.namespace, "selector", "selector-test:" + x)
      cl.put(null, key, name, age, colour, animal)
      i += 1
      if (i == 5) i = 0
    }

    i = 0
    for (x <- 1 to 100) {
      val name = new Bin("name", "name:" + i)
      val age = new Bin("age", ages(i))
      val colour = new Bin("color", colours(i))
      val animal = new Bin("animal", animals(i))
      val key = new Key(Globals.namespace, "selectorInt", x)
      cl.put(null, key, name, age, colour, animal)
      i += 1
      if (i == 5) i = 0
    }

  }

  it should "Scan Set" in {
    val c = session.scanSet("selector").count
    assert(c == 100)
  }

  it should "clean up because it's mother doesn't work here" in {
    val cl = AerospikeConnection.getClient(conf)

    var i = 0
    for (x <- 1 to 100) {
      val name = new Bin("name", "name:" + i)
      val age = new Bin("age", ages(i))
      val colour = new Bin("color", colours(i))
      val animal = new Bin("animal", animals(i))
      val key = new Key(Globals.namespace, "selector", "selector-test:" + x)
      cl.delete(null, key)
      i += 1
      if (i == 5) i = 0
    }

    i = 0
    for (x <- 1 to 100) {
      val name = new Bin("name", "name:" + i)
      val age = new Bin("age", ages(i))
      val colour = new Bin("color", colours(i))
      val animal = new Bin("animal", animals(i))
      val key = new Key(Globals.namespace, "selectorInt", x)
      cl.delete(null, key)
      i += 1
      if (i == 5) i = 0
    }
  }
}