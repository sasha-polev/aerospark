
package com.aerospike.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

class DefaultSource extends RelationProvider {

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    val seedHost = parameters(AerospikeConfig.SeedHost)
    val port = parameters(AerospikeConfig.Port)
    val conf = new AerospikeConfig(seedHost, port)
    AerospikeRelation(conf)(sqlContext)
  }
}