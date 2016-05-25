
package com.aerospike.spark.sql

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType

import com.aerospike.client.policy.WritePolicy



class DefaultSource extends RelationProvider
with Logging
with CreatableRelationProvider{

	override def createRelation(sqlContext: SQLContext, 
			parameters: Map[String, String]): BaseRelation = {
					parameters.getOrElse(AerospikeConfig.SeedHost, sys.error(AerospikeConfig.SeedHost + " must be specified"))
					parameters.getOrElse(AerospikeConfig.Port, sys.error(AerospikeConfig.Port + " must be specified"))
					parameters.getOrElse(AerospikeConfig.NameSpace, sys.error(AerospikeConfig.NameSpace + " must be specified"))
					logInfo("Creating Aerospike relation for " + AerospikeConfig.NameSpace +":"+ AerospikeConfig.SetName)
					val conf = AerospikeConfig.apply(parameters)
					val ref = new AerospikeRelation(conf, null)(sqlContext)
					return ref
	}

	override def createRelation(
			sqlContext: SQLContext,
			mode: SaveMode,
			parameters: Map[String, String],
			data: DataFrame): BaseRelation = {

			val conf = AerospikeConfig.apply(parameters)

			val wp = writePolicy(mode, conf)

			saveDataFrame(data, mode, conf, wp)

			createRelation(sqlContext, parameters)
	}

	private def writePolicy(mode: SaveMode, config: AerospikeConfig): WritePolicy = {
			val hasUpdateByKey = config.get(AerospikeConfig.UpdateByKey) != null
			val hasUpdateByDigest = config.get(AerospikeConfig.UpdateByDigest) != null
      val hasUpdate = hasUpdateByKey || hasUpdateByDigest

    if(hasUpdateByDigest && hasUpdateByKey){
      sys.error("Cannot use hasUpdateByKey and hasUpdateByDigest configuration together")
    }
			
		 mode match {
			case SaveMode.ErrorIfExists => new WritePolicy() // TODO
			case SaveMode.Ignore => new WritePolicy() // TODO
			case SaveMode.Overwrite => new WritePolicy() // TODO
			case SaveMode.Append => new WritePolicy() // TODO
		}
	}

	def saveDataFrame(data: DataFrame, mode: SaveMode, config: AerospikeConfig, writePolicy: WritePolicy){
		val schema = data.schema
				data.foreachPartition { iterator =>
				savePartition(iterator, schema, mode, config, writePolicy) }
	}

	private def savePartition(iterator: Iterator[Row],
			schema: StructType, mode: SaveMode, config: AerospikeConfig, writePolicy: WritePolicy): Unit = {  

					logDebug("fetch client to save partition")
					val client = AerospikeConnection.getClient(config)
					var counter = 0
					while (iterator.hasNext) {
						val row = iterator.next()
								saveRow(row, null)
								counter += 1;         
					}
					logDebug(s"Completed writing partition of $counter rows")
	}

	private def saveRow(row: Row, policy: WritePolicy): Unit = {
	  println(row)
	}

}