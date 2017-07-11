package com.aerospike.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.Value
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.types.IntegerType
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SaveMode

import com.aerospike.client.policy.WritePolicy
import com.aerospike.spark.SparkASITSpecBase
import com.aerospike.spark._


class AerospikeRelationTest extends FlatSpec with BeforeAndAfter with SparkASITSpecBase{
  var client: AerospikeClient = _

  val TEST_COUNT = 100

  before {
    createTestData()
  }

  def createTestData() = {
    client = AerospikeConnection.getClient(conf)
    Value.UseDoubleType = true
    val wp = new WritePolicy()
    wp.expiration = 600 // expire data in 10 minutes
    for (i <- 1 to TEST_COUNT) {
      val key = new Key(Globals.namespace, "rdd-test", s"rdd-test-$i")
      client.put(wp, key,
        new Bin("one", i),
        new Bin("two", s"two:$i"),
        new Bin("three", i.toDouble)
      )
    }
  }

  behavior of "Aerospike Relation"

  it should "create an AerospikeRelation" in {
    val spark = session
    import spark.implicits._

    val thingsDF = spark.scanSet("rdd-test")
    val result = thingsDF.take(50)
    result.foreach { row =>
      assert(row.getAs[String]("two").startsWith("two:"))
    }
  }

  it should " select the data using filter on 'one'" in {
    val thingsDF = sqlContext.read
      .format("com.aerospike.spark.sql")
      .option("aerospike.set", "rdd-test")
      .load
    thingsDF.createOrReplaceTempView("things")
    val filteredThings = sqlContext.sql("select * from things where one = 55")
    val count = filteredThings.count()
    assert(count > 0)
    val thing = filteredThings.first()
    val one = thing.getAs[Long]("one")
    assert(one == 55)
  }

  it should " select the data using range filter where 'one' is between 55 and 65" in {
    val thingsDF = sqlContext.read
      .format("com.aerospike.spark.sql")
      .option("aerospike.set", "rdd-test")
      .load
    thingsDF.createOrReplaceTempView("things")
    val filteredThings = sqlContext.sql("select * from things where one between 55 and 65")
    val count = filteredThings.count()
    assert(count > 0)
    val thing = filteredThings.first()
    val one = thing.getAs[Long]("one")
    assert(one >= 55)
    assert(one <= 65)
  }

  it should "save with Overwrite (RecordExistsAction.REPLACE)" in {
    val thingsDF = sqlContext.read
      .format("com.aerospike.spark.sql")
      .option("aerospike.set", "rdd-test")
      .load
    thingsDF.write
      .mode(SaveMode.Overwrite)
      .format("com.aerospike.spark.sql")
      .option("aerospike.set", "rdd-test")
      .option("aerospike.updateByDigest", "__digest")
      .save()
  }

  it should "save with Ignore (RecordExistsAction.CREATE_ONLY)" in {
    val thingsDF = sqlContext.read
      .format("com.aerospike.spark.sql")
      .option("aerospike.set", "rdd-test")
      .load
    thingsDF.write
      .mode(SaveMode.Ignore)
      .format("com.aerospike.spark.sql")
      .option("aerospike.set", "rdd-test")
      .option("aerospike.updateByDigest", "__digest")
      .save()
  }

  it should "write data from DataFrame with expiry" in {

    val setName = "new-rdd-data"

    val schema = new StructType(Array(
      StructField("key",StringType,nullable = false),
      StructField("last",StringType,nullable = true),
      StructField("first",StringType,nullable = true),
      StructField("when",LongType,nullable = true),
      StructField("ttl", IntegerType, nullable = true)
    ))
    val rows = Seq(
      Row("Fraser_Malcolm","Fraser", "Malcolm", 1975L, 600),
      Row("Hawke_Bob","Hawke", "Bob", 1983L, 600),
      Row("Keating_Paul","Keating", "Paul", 1991L, 600),
      Row("Howard_John","Howard", "John", 1996L, 600),
      Row("Rudd_Kevin","Rudd", "Kevin", 2007L, 600),
      Row("Gillard_Julia","Gillard", "Julia", 2010L, 600),
      Row("Abbott_Tony","Abbott", "Tony", 2013L, 600),
      Row("Tunrbull_Malcom","Tunrbull", "Malcom", 2015L, 600)
    )

    val inputRDD = sc.parallelize(rows)

    val newDF = sqlContext.createDataFrame(inputRDD, schema)

    newDF.write
      .mode(SaveMode.Ignore)
      .format("com.aerospike.spark.sql")
      .option("aerospike.set", setName)
      .option("aerospike.updateByKey", "key")
      .option("aerospike.ttlColumn", "ttl")  // new time to live from colum
      .save()

    var key = new Key(Globals.namespace, setName, "Fraser_Malcolm")
    var record = client.get(null, key)
    assert(record.getString("last") == "Fraser")

    key = new Key(Globals.namespace, setName, "Hawke_Bob")
    record = client.get(null, key)
    assert(record.getString("first") == "Bob")

    key = new Key(Globals.namespace, setName, "Gillard_Julia")
    record = client.get(null, key)
    assert(record.getLong("when") == 2010)
  }

  it should "write and read a lot of data" in {
    /*
     * Read flights data from CSV file an load them if they don't exist
     */
    val checkKey: Key = new Key(Globals.namespace, "spark-test", Flight.formKey("2313", "AA", "655", "20151-1-10"))

    if (!client.exists(null, checkKey)) {
      val rawFlightsRDD = sc.textFile("data/flightsaa.csv")
      /*
       * Parse each line into a Flight case class RDD
       */
      val flightsRDD = rawFlightsRDD
        .filter(!_.contains("YEAR")) // Ignore headers
        .map(_.replace("\"", ""))
        .map(Flight.assign)
        .filter(_.DEP_TIME != null) // flights that never depart
        .filter(_.ARR_TIME != null) // flights that never arrive

      /*
       * make a DataFrame from the RDD
       */
      val flightsDF = sqlContext.createDataFrame(flightsRDD)

      flightsDF.write.aerospike
        .mode(SaveMode.Overwrite)
        .setName("spark-test")
        .key("key")
        .option("aerospike.ttlColumn", "expiry")
        .save()
    }
    /*
     * find all the flights that are > 5 minutes late
     */
    val readFlightsDF = sqlContext.read
      .format("com.aerospike.spark.sql")
      .option("aerospike.set", "spark-test")
      .load

    readFlightsDF.registerTempTable("Flights")

    val lateFlightsDF = sqlContext.sql("select CARRIER, FL_NUM, DEP_DELAY_NEW, ARR_DELAY_NEW from Flights where ARR_DELAY_NEW > 5")

    assert(12907 == lateFlightsDF.count())
  }
}

case class Flight(
  YEAR: Int,
  MONTH: Int,
  DAY_OF_MONTH: Int,
  DAY_OF_WEEK: Int,
  FL_DATE:java.sql.Date,
  CARRIER: String,
  TAIL_NUM: String,
  FL_NUM: String,
  ORIG_ID: Int,
  ORIG_SEQ_ID: Int,
  ORIGIN: String,
  DEST_AP_ID: Int,
  DEST: String,
  DEP_TIME: java.sql.Date,
  DEP_DELAY_NEW: Double,
  ARR_TIME: java.sql.Date,
  ARR_DELAY_NEW: Double,
  ELAPSED_TIME: Double,
  DISTANCE: Double,
  key: String,
  expiry: Int
) extends Serializable

object Flight{
  val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")
  val tf = DateTimeFormat.forPattern("yyyy-MM-dd HHmm")

  def assign(csvRow: String): Flight = {

    val values = csvRow.split(",")
    val flight = new Flight(
      values(0).toInt,
      values(1).toInt,
      values(2).toInt,
      values(3).toInt,
      new java.sql.Date(dtf.parseDateTime(values(4)).getMillis),
      values(5),
      values(6),
      values(7),
      values(8).toInt,
      values(9).toInt,
      values(10),
      values(11).toInt,
      values(12),
      toTime(values(4), values(13)),
      toDouble(values(14)),
      toTime(values(4), values(15)),
      toDouble(values(16)),
      toDouble(values(17)),
      toDouble(values(18)),
      formKey(values),
      -1
    )
    flight
  }

  def formKey(values:Array[String]): String = {
    val dep = formKey(values(13),
      values(5),
      values(7),
      values(4))
    dep
  }

  def formKey(depTime:String, carrier:String, flNumber:String, flDate:String): String = {
    val dep = if (depTime.isEmpty) "XXXX" else depTime
    carrier+flNumber+":"+flDate + ":" + dep
  }

  def toDouble(doubleString: String): Double = {
    val number = doubleString match {
      case "" => 0.0
      case _ => doubleString.toDouble
    }
    number
  }

  def toTime(dateString:String, timeString: String): java.sql.Date = {
    val time = timeString match {
      case "" => null
      case "2400" => new java.sql.Date(tf.parseDateTime(dateString + " 0000").getMillis)
      case _ => new java.sql.Date(tf.parseDateTime(dateString + " " + timeString).getMillis)
    }
    time
  }
}
