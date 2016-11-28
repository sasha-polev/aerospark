 package com.aerospike.spark.sql

 import org.apache.spark.SparkContext
 import org.apache.spark.sql.{SQLContext, SparkSession}
 import org.scalatest.{BeforeAndAfterAll, Suite}

 trait SparkTest extends BeforeAndAfterAll { self: Suite =>

   var sc: SparkContext = _
   var session: SparkSession = _
   var sqlContext: SQLContext = _

   override def beforeAll(): Unit = {
     super.beforeAll()
     session = SparkSession.builder()
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
