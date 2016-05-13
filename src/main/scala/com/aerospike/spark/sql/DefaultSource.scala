
package com.aerospike.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider  {

  override def createRelation(sqlContext: SQLContext, 
      parameters: Map[String, String]): BaseRelation = {
   parameters.getOrElse(AerospikeConfig.SeedHost, sys.error(AerospikeConfig.SeedHost + " must be specified"))
   parameters.getOrElse(AerospikeConfig.Port, sys.error(AerospikeConfig.Port + " must be specified"))
   parameters.getOrElse(AerospikeConfig.NameSpace, sys.error(AerospikeConfig.NameSpace + " must be specified"))
   val conf = AerospikeConfig.apply(parameters, 
       List(AerospikeConfig.SeedHost, AerospikeConfig.Port, AerospikeConfig.NameSpace, AerospikeConfig.SetName))
   val ref = new AerospikeRelation(conf, null)(sqlContext)
   return ref
 }
  
}