package com.aerospike.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.scalatest.{ BeforeAndAfterAll, Suite }
import org.apache.spark.SparkConf
import org.scalatest.FlatSpec
import org.testcontainers.containers.wait.Wait
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.BindMode

trait SparkASITSpecBase extends FlatSpec with BeforeAndAfterAll { self: Suite =>

  var sc: SparkContext = _
  var session: SparkSession = _
  var sqlContext: SQLContext = _
  var conf: SparkConf = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
   
    conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Aerospike Tests for Spark Dataset")
      .set("aerospike.seedhost", Globals.seedHost)
      .set("aerospike.port",  SparkASContainerSpec.container.getMappedPort(Globals.port).toString)
      .set("aerospike.namespace", Globals.namespace)
      .set("stream.orig.url", "localhost")
      
    session = SparkSession.builder()
    .config(conf)
    .master("local[*]")
    .appName("Aerospike Tests")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
    
    sc = session.sparkContext
    sqlContext = session.sqlContext
   }

  override def afterAll(): Unit = {
    if (session != null) {
      session.stop()
    }
    
    super.afterAll()
  }
}
