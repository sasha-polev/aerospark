package com.aerospike.spark.sql

import java.util

import org.apache.spark.sql.Row
import org.scalatest.FlatSpec
import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.Value
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.MapType
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SaveMode
import com.aerospike.client.policy.WritePolicy

import scala.util.Random


class MapTest extends FlatSpec with BeforeAndAfter with SparkTest {

  val config = AerospikeConfig.newConfig(Globals.seedHost, 3000, 1000)
  var client: AerospikeClient = _
  val set = "maps"
  val mapBin = "map-of-things"

  val TEST_COUNT = 100

  before {
    createTestData()
  }

  def createTestData(): Unit = {
    client = AerospikeConnection.getClient(config)
    Value.UseDoubleType = true
    val wp = new WritePolicy()
    wp.expiration = 600 // expire data in 10 minutes

    // Create many records with values in a map
    val rand = new Random(300)
    for (i <- 1 to TEST_COUNT){
      val newKey = new Key(Globals.namespace, set, s"a-record-with-a-map-$i")
      val aMap = new util.HashMap[String, Long]()
      for ( j <- 1 to TEST_COUNT){
        val newInt = rand.nextInt(200) + 250L
        aMap.put(s"index-$j", newInt)
      }
      //println(aMap)
      client.put(wp, newKey, new Bin(mapBin, aMap))
    }
  }

  behavior of "Aerospike Map"

  it should "read map data" in {
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
      val position = row.fieldIndex(mapBin)
      val map = row.getMap[String, Long](position)
      if ( map.contains("index-9")){
        val first = map.getOrElse("index-9", -1L)
        assert(first != -1)
      } else if ( map.contains("from")){
        val first = map.getOrElse("from", -1L)
        assert(first != -1)
      }
    }
  }

  it should "write map data" in {
    val schema = new StructType(Array(
      StructField("key", StringType, nullable = false),
      StructField("last", StringType, nullable = true),
      StructField("first", StringType, nullable = true),
      StructField(mapBin, MapType(StringType, LongType)),
      StructField("ttl", IntegerType, nullable = true)
    ))
    val rows = Seq(
      Row("Fraser_Malcolm","Fraser", "Malcolm", Map("from" -> 1975L, "to" -> 1983L), 600),
      Row("Hawke_Bob","Hawke", "Bob", Map("from" -> 1983L, "to" -> 1991L), 600),
      Row("Keating_Paul","Keating", "Paul", Map("from" -> 1991L, "to" -> 1996L), 600),
      Row("Howard_John","Howard", "John", Map("from" -> 1996L, "to" -> 2007L), 600),
      Row("Rudd_Kevin","Rudd", "Kevin", Map("from" -> 2007L, "to" -> 2010L), 600),
      Row("Gillard_Julia","Gillard", "Julia", Map("from" -> 2010L, "to" -> 2013L), 600),
      Row("Abbott_Tony","Abbott", "Tony", Map("from" -> 2013L, "to" -> 2015L), 600),
      Row("Tunrbull_Malcom","Tunrbull", "Malcom", Map("from" -> 2015L, "to" ->  2016L), 600)
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
    val fraserMap = fraser.getMap(mapBin)
    assert(fraserMap.get("from") == 1975L)

    poliKey = new Key(Globals.namespace, set, "Gillard_Julia")
    val gillard = client.get(null, poliKey)
    assert(gillard != null)
    val gillardMap = gillard.getMap(mapBin)
    assert(gillardMap.get("to") == 2013L)

    poliKey = new Key(Globals.namespace, set, "Tunrbull_Malcom")
    val tunrbull = client.get(null, poliKey)
    assert(tunrbull != null)
    val tunrbullMap = tunrbull.getMap(mapBin)
    assert(tunrbullMap.get("to") == 2016L)
  }
}
