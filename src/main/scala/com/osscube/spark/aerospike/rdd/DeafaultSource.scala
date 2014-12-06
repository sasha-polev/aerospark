package com.osscube.spark.aerospike.rdd

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

class DefaultSource extends RelationProvider {

  /**
   * Creates a new relation for aerospike select statememnt.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    if(parameters.contains("partitionsPerServer") && !parameters("partitionsPerServer").isEmpty)
      AeroRelation(parameters("initialHost"), parameters("select"), parameters("partitionsPerServer").toInt)(sqlContext)
    else
      AeroRelation(parameters("initialHost"), parameters("select"))(sqlContext)
  }
}