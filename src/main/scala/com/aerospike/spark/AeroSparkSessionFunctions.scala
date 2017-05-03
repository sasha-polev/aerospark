package com.aerospike.spark

import org.apache.spark.sql.SparkSession
import com.aerospike.spark.sql.AerospikeConfig
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

class AeroSparkSessionFunctions(val sparkSession: SparkSession) extends AnyVal {

  /**
   * Utilized Aerospike batch read for dataset join
   */
  def scanSet(set: String): Dataset[Row] = {
    sparkSession.read.aerospikeFormat.option("aerospike.set", set).load
  }

}