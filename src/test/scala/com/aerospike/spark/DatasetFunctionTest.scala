package com.aerospike.spark

import scala.collection.convert.Wrappers.JMapWrapper

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.policy.WritePolicy
import com.aerospike.spark.sql.AerospikeConnection
import com.aerospike.spark.sql.SparkTest
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer

class DatasetFunctionTest extends FlatSpec with Matchers with SparkASITSpecBase {
  val ages = Array(25, 26, 27, 28, 29)
  val colours = Array("blue", "red", "yellow", "green", "orange")
  val animals = Array("cat", "dog", "mouse", "snake", "lion")
  val wp = new WritePolicy()
  wp.expiration = 600

  behavior of "Dataset Func for spark"

  it should "Get a connected client from cache" in {
     val client = AerospikeConnection.getClient(conf)
    client shouldBe a[AerospikeClient]
    assert(client.isConnected())
  }

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

  it should "Select by BatchJoin String" in {
    val spark = session
    import spark.implicits._
    val ds = Seq(
      Data(1, "selector-test:20"),
      Data(2, "selector-test:31")).toDS()

    val it = ds.batchJoin("b", "selector")
    for (row <- it.values) {
      val key = row.getValue("name")
      assert(Seq("name:4", "name:0").contains(key))
    }
  }

  it should "Select by BatchJoin Int" in {
    val spark = session
    import spark.implicits._
    
    val ds = Seq(
      IntData(1, 20),
      IntData(2, 32)).toDS()

    val it = ds.batchJoin("b", "selectorInt")
    for (row <- it.values) {
      val key = row.getValue("name")
      assert(Seq("name:4", "name:1").contains(key))
    }
  }

  it should "Select by aeroJoin Int" in {     
    val spark = session
    import spark.implicits._

    val ds = Seq(
      IntData(1, 20),
      IntData(2, 32)).toDS()

    val it = ds.aeroJoin("b", "selectorInt")
    it.foreach { row =>
      val key1 = row.get(1).asInstanceOf[JMapWrapper[String, Any]]
      val key2 = key1.get("name").get
      
      assert(Seq("name:4", "name:1").contains(key2))
    }
  }

  it should "Select by Intersect" in {
    val spark = session
    import spark.implicits._
    
    val ds = Seq(
      IntersectData("selector-test:20", "orange", "name:4", "lion", 29),
      IntersectData("selector-test:31", "orange", "name:0", "lion", 33)).toDS()

    ds.aeroIntersect("key", "selector").collect.foreach { data =>
      assert(data.name.equals("name:4"))
    }
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

case class Data(a: Int, b: String)
case class IntData(a: Int, b: Int)
case class IntersectData(key: String, color: String, name: String, animal: String, age: Int)
