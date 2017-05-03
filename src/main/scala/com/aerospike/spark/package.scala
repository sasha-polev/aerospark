package com.aerospike

import org.apache.spark.sql.Dataset
import com.aerospike.spark.sql.AerospikeConfig
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.SparkSession

package object spark {
  
  implicit def toDatasetFunctions[T](dataset: Dataset[T]): AeroSparkDatasetFunctions[T] = new AeroSparkDatasetFunctions(dataset)
  
    /** 
  *  Returns a map of configuration for aesropike client connection
  *  
  * */
  def aerospikeConfs(
      set: String,
      namespace: String,
      seedHost: String = AerospikeConfig.DEFAULT_SEED_HOST): Map[String, String] =
    Map(
      AerospikeConfig.SeedHost -> seedHost,
      AerospikeConfig.NameSpace -> namespace,
      AerospikeConfig.SetName -> set)
  
  implicit class DataFrameReaderWrapper(val dfReader: DataFrameReader) extends AnyVal {
    /** Sets the format used to access Aerospike through Client */
    def aerospikeFormat: DataFrameReader = {
      dfReader.format("com.aerospike.spark.sql")
    }

    /** Sets the format used to access Cassandra through Connector and configure a path to Cassandra table. */
    def aerospikeFormat(
        set: String,
        namespace: String,
        seedHost: String = AerospikeConfig.DEFAULT_SEED_HOST): DataFrameReader = {

      aerospikeFormat.options(aerospikeConfs(set, namespace, seedHost))
    }
  }
  
  implicit class DataFrameWriterWrapper[T](val dfWriter: DataFrameWriter[T]) extends AnyVal {
    /** Sets the format used to access Aerospike through client */
    def aerospikeFormat: DataFrameWriter[T] = {
      dfWriter.format("com.aerospike.spark.sql")
    }

    /** Sets the format used to access Aerospike set */
    def aerospikeFormat(
       set: String,
       key: String): DataFrameWriter[T] = {

      aerospikeFormat
        .option(AerospikeConfig.SetName, set)
        .option(AerospikeConfig.UpdateByKey, key)
    }
  }
  
  implicit def toAerospikeSessionFunctions(sparkSession: SparkSession): AeroSparkSessionFunctions = new AeroSparkSessionFunctions(sparkSession)
}
