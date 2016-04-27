package com.objy.spark.sql

import org.apache.spark.sql.sources.{RelationProvider, BaseRelation, CreatableRelationProvider}
import org.apache.spark.sql.{SQLContext, SaveMode, DataFrame}
import org.apache.spark.Logging
import com.aerospike.spark.sql.AerospikeConfig


/**
 * Used by SQLContext to invoke the Objy specific RelationProvider
 */
class DefaultSource extends RelationProvider 
  with CreatableRelationProvider
  with Logging {

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {

    val requiredProps = List(AerospikeConfig.SeedHost, AerospikeConfig.Port)

    val aerospikeConfig = AerospikeConfig(parameters, requiredProps)
    logInfo("Creating Aerospike relation for class " + AerospikeConfig.get(AerospikeConfig.SeedHost))
    AerospikeRelation(aerospikeConfig)(sqlContext)
  }
  
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
        
    val requiredProps = List(AerospikeConfig.SeedHost, AerospikeConfig.Port)
    val aerospikeConfig = AerospikeConfig(parameters, requiredProps)
    validateWriteConfig(mode, aerospikeConfig)
    
    val writer = AerospikeWriter
    if(writer.prepareDatabase(data.schema, mode, Config))
      writer.writeDataFrame(data, mode, aerospikeConfig)
    
    // Create relation for the resulting set (perhaps make optional ?)
    // Note internal create expects properties as a "CaseInsensitive" map
    createRelation(sqlContext, parameters)
  }
  
  private def validateWriteConfig(mode: SaveMode, config: AerospikeConfig) = {
    // ensure any update modes are used correctly
    val hasUpdateByKey = config.get(AerospikeConfig.UpdateByKey) != None
    val hasUpdate = hasUpdateByKey
    
    
    if(hasUpdate) mode match {
      case SaveMode.ErrorIfExists | SaveMode.Ignore =>
        sys.error("updateByKey may only be used with SaveMode's Overwrite & Append")
      case _ => // continue
    }
  }
}
