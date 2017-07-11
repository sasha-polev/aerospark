package com.aerospike.spark

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.policy.WritePolicy
import com.aerospike.spark.sql.AerospikeConnection

class AeroSparkDatasetFunctionsSpec extends FlatSpec with Matchers with SparkASITSpecBase {
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
      val name = new Bin("name", "name_" + i)
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
      val name = new Bin("name", "name_" + i)
      val age = new Bin("age", ages(i))
      val colour = new Bin("color", colours(i))
      val animal = new Bin("animal", animals(i))
      val key = new Key(Globals.namespace, "selectorInt", x)
      cl.put(null, key, name, age, colour, animal)
      i += 1
      if (i == 5) i = 0
    }

  }
  
  it should "Save to Aerospike" in {
    val spark = session
    import spark.implicits._
    val ds = Seq(
      Data(1, "selector-test:20"),
      Data(3, "selector-test:21"),
      Data(2, "selector-test:31")
    ).toDS()

    ds.write.aerospike.setName("inserts").key("a").save
    assert(spark.scanSet("inserts").count == 3)
  }

  it should "Select by aeroJoinMap Int" in {     
    val spark = session
    import spark.implicits._

    val ds = Seq(
      IntData(1, 20),
      IntData(2, 32)).toDS()

    val m = ds.aeroJoinMap("b", "selectorInt")
    m.foreach { t =>
      val key = t._2.get("name").get
      assert(Seq("name_4", "name_1").contains(key))
    }
  }
  
    it should "Select by aeroJoin Generic" in {     
    val spark = session
    import spark.implicits._

    var ds = Seq(
      IntData(1, 20),
      IntData(2, 32)).toDS()

    var m = ds.aeroJoin[GTest]("b", "selectorInt")
    assert(m.count()==2)
    
    ds = Seq(
      IntData(1, 20),
      IntData(2, 132)).toDS()

    m = ds.aeroJoin[GTest]("b", "selectorInt")
    println("Generic aeroJoin result: ")
    m.foreach(f => println( f))
    assert(m.count()==1)
    
    ds = Seq(
      IntData(1, 120),
      IntData(2, 132)).toDS()

    m = ds.aeroJoin[GTest]("b", "selectorInt")
    assert(m.count()==0)
    
  }

  it should "Select by Intersect" in {
    val spark = session
    import spark.implicits._
    
    val ds = Seq(
      IntersectData("selector-test:20", "orange", "name_4", "lion", 29),
      IntersectData("selector-test:31", "orange", "name_0", "lion", 33)).toDS()

    ds.aeroIntersect("key", "selector").collect.foreach { data =>
      assert(data.name.equals("name_4"))
    }
  }

  it should "clean up because it's mother doesn't work here" in {
    val cl = AerospikeConnection.getClient(conf)

    var i = 0
    for (x <- 1 to 100) {
      val name = new Bin("name", "name_" + i)
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
      val name = new Bin("name", "name_" + i)
      val age = new Bin("age", ages(i))
      val colour = new Bin("color", colours(i))
      val animal = new Bin("animal", animals(i))
      val key = new Key(Globals.namespace, "selectorInt", x)
      cl.delete(null, key)
      i += 1
      if (i == 5) i = 0
    }
  }
  
  it should "instantiate class from a Map" in {
      assert(fromMap[Test](Map("t" -> "test", "ot" -> "test2", "ott"->"test 3")) === new Test("test", Some("test 3")))
      assert(fromMap[CaseTest](Map("t" -> "test", "ot" -> "test2", "ott"->"test 3")) === CaseTest("test", Some("test 3")))
      assert(fromMap[CaseTest](Map("t" -> "test", "ott" -> "test2")) === CaseTest("test", Some("test2")))
      assert(fromMap[CaseTest](Map("t" -> "test")) === CaseTest("test", None))
  }
}
case class GTest(name: String, age: Long, color:String, __key: Any) extends AeroKV
 
case class CaseTest(t: String, ott: Option[String])
class Test(var t: String, var ott: Option[String]) extends Equals {
  def canEqual(other: Any) = {
    other.isInstanceOf[com.aerospike.spark.Test]
  }

  override def equals(other: Any) = {
    other match {
      case that: com.aerospike.spark.Test => that.canEqual(Test.this) && t == that.t && ott == that.ott
      case _ => false
    }
  }

  override def hashCode() = {
    val prime = 41
    prime * (prime + t.hashCode) + ott.hashCode
  }
}
