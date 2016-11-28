package com.aerospike.spark.sql

import java.util

import scala.util.Random
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.Value
import com.aerospike.client.policy.WritePolicy


class ListTest extends FlatSpec with BeforeAndAfter with SparkTest {

  val config = AerospikeConfig.newConfig(Globals.seedHost, 3000, 1000)
  var client: AerospikeClient = _
  val set = "lists"
  val listBin = "list-of-things"

  val TEST_COUNT = 100

  before {
    createTestData()
  }

  def createTestData(): Unit = {
    client = AerospikeConnection.getClient(config)
    Value.UseDoubleType = true
    val wp = new WritePolicy()
    wp.expiration = 600 // expire data in 10 minutes

    // Create many records with values in a list
    val rand = new Random(300)
    for (i <- 1 to TEST_COUNT){
      val newKey = new Key(Globals.namespace, set, s"a-record-with-a-list-$i")
      val aList = new util.ArrayList[Long]()
      for ( j <- 1 to TEST_COUNT) {
        val newInt = rand.nextInt(200) + 250L
        aList.add(newInt)
      }
      client.put(wp, newKey, new Bin(listBin, aList))
    }
  }

  behavior of "Aerospike List"

  it should "read list data" in {
    val thingsDF = sqlContext.read.
      format("com.aerospike.spark.sql").
      option("aerospike.seedhost", Globals.seedHost).
      option("aerospike.port", Globals.port.toString).
      option("aerospike.namespace", Globals.namespace).
      option("aerospike.set", set).
      load
    thingsDF.printSchema()
    //thingsDF.show()

    val result = thingsDF.take(50)
    result.foreach { row =>
      val position = row.fieldIndex(listBin)
      val list = row.getList[Long](position)
      val first = list.get(0)
      assert(first.isInstanceOf[Long])
    }
  }

  it should "write list data" in {
    val schema = new StructType(Array(
      StructField("key", StringType, nullable = false),
      StructField("last", StringType, nullable = true),
      StructField("first", StringType, nullable = true),
      StructField(listBin, ArrayType(LongType, containsNull = true)),
      StructField("ttl", IntegerType, nullable = true)
    ))
    val rows = Seq(
      Row("Fraser_Malcolm","Fraser", "Malcolm", Array(1975L, 1983L), 600),
      Row("Hawke_Bob","Hawke", "Bob", Array(1983L, 1991L), 600),
      Row("Keating_Paul","Keating", "Paul", Array(1991L, 1996L), 600),
      Row("Howard_John","Howard", "John", Array(1996L, 2007L), 600),
      Row("Rudd_Kevin","Rudd", "Kevin", Array(2007L, 2010L), 600),
      Row("Gillard_Julia","Gillard", "Julia", List(2010L, 2013L), 600),
      Row("Abbott_Tony","Abbott", "Tony", Array(2013L, 2015L), 600),
      Row("Tunrbull_Malcom","Tunrbull", "Malcom", Seq(2015L, 2016L), 600)
    )

    val inputRDD = sc.parallelize(rows)

    val newDF = sqlContext.createDataFrame(inputRDD, schema)

    newDF.write.
      mode(SaveMode.Overwrite).
      format("com.aerospike.spark.sql").
      option("aerospike.seedhost", Globals.seedHost).
      option("aerospike.port", Globals.port.toString).
      option("aerospike.namespace", Globals.namespace).
      option("aerospike.set", set).
      option("aerospike.updateByKey", "key").
      option("aerospike.ttlColumn", "ttl").
      save()

    var poliKey = new Key(Globals.namespace, set, "Fraser_Malcolm")
    val fraser = client.get(null, poliKey)
    assert(fraser != null)
    val fraserList = fraser.getList(listBin)
    assert(fraserList.get(0) == 1975L)

    poliKey = new Key(Globals.namespace, set, "Gillard_Julia")
    val gillard = client.get(null, poliKey)
    assert(gillard != null)
    val gillardList = gillard.getList(listBin)
    assert(gillardList.get(1) == 2013L)

    poliKey = new Key(Globals.namespace, set, "Tunrbull_Malcom")
    val tunrbull = client.get(null, poliKey)
    assert(tunrbull != null)
    val tunrbullList = tunrbull.getList(listBin)
    assert(tunrbullList.get(1) == 2016L)
  }
}
