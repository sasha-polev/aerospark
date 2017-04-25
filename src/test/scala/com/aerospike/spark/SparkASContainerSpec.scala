package com.aerospike.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.scalatest.{ BeforeAndAfterAll, Suite }
import org.apache.spark.SparkConf
import org.scalatest.FlatSpec
import org.testcontainers.containers.wait.Wait
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.BindMode

object SparkASContainerSpec extends FlatSpec { self: Suite =>
  val container = new GenericContainer("aerospike:latest")
  container.withExposedPorts(Globals.port)
  container.withClasspathResourceMapping("aerospike.conf", "/etc/aerospike/aerospike.conf", BindMode.READ_ONLY)
  container.start
}
