package com.aerospike.spark

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


class AeroSparkSessionFunctions(val sparkSession: SparkSession) extends AnyVal {

  /**
   * Utilized Aerospike batch read for dataset join
   */
  def scanSet(set: String): Dataset[Row] = {
    aeroRead.option("aerospike.set", set).load
  }

  def aeroRead(): DataFrameReader = {
    sparkSession.read.aerospike
  }
 
}