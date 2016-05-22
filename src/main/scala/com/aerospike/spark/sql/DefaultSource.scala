
package com.aerospike.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import org.apache.spark.Logging

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
					val requiredPramaters = List(AerospikeConfig.SeedHost, 
							AerospikeConfig.Port,
							AerospikeConfig.NameSpace)
					val conf = AerospikeConfig.apply(parameters, 
							requiredPramaters)
					val ref = new AerospikeRelation(conf, null)(sqlContext)
					return ref
	}

	override def createRelation(
			sqlContext: SQLContext,
			mode: SaveMode,
			parameters: Map[String, String],
			data: DataFrame): BaseRelation = {

					val requiredPramaters = List(AerospikeConfig.SeedHost, 
							AerospikeConfig.Port,
							AerospikeConfig.NameSpace)
							val conf = AerospikeConfig.apply(parameters, 
									requiredPramaters)

							checkWriteConf(mode, conf)

							saveDataFrame(data, mode, conf)

							createRelation(sqlContext, parameters)
	}

	private def checkWriteConf(mode: SaveMode, config: AerospikeConfig) = {
			val hasUpdateByKey = !config.get(AerospikeConfig.UpdateByKey).asInstanceOf[String].isEmpty()

					if(hasUpdateByKey) mode match {
					case SaveMode.ErrorIfExists | SaveMode.Ignore =>
					sys.error("updateByKey can only be used with SaveMode's Overwrite & Append")
					case _ => // continue
					}
	}

	def saveDataFrame(data: DataFrame, mode: SaveMode, config: AerospikeConfig){
		val schema = data.schema
				data.foreachPartition { iterator =>
				savePartition(iterator, schema, mode, config) }
	}

	private def savePartition(iterator: Iterator[Row],
			schema: StructType, mode: SaveMode, config: AerospikeConfig): Unit = {  

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