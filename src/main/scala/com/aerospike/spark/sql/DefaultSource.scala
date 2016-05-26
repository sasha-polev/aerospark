
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
import com.aerospike.client.policy.RecordExistsAction
import com.aerospike.client.Key
import com.aerospike.client.Value
import com.aerospike.client.Bin
import scala.collection.mutable.ListBuffer



class DefaultSource extends RelationProvider with Serializable
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
			
		val policy = new WritePolicy
		mode match {
			case SaveMode.ErrorIfExists => policy.recordExistsAction = RecordExistsAction.CREATE_ONLY
			case SaveMode.Ignore => policy.recordExistsAction = RecordExistsAction.CREATE_ONLY
			case SaveMode.Overwrite => policy.recordExistsAction = RecordExistsAction.UPDATE
			case SaveMode.Append => policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY
		}
		policy
		
	}

	def saveDataFrame(data: DataFrame, mode: SaveMode, config: AerospikeConfig, writePolicy: WritePolicy){
		val schema = data.schema
				data.foreachPartition { iterator =>
				savePartition(iterator, schema, mode, config) }
	}

	private def savePartition(iterator: Iterator[Row],
			schema: StructType, mode: SaveMode, config: AerospikeConfig): Unit = { 
	  
	  val metaFields = Set(config.keyColumn(),
        config.digestColumn(), 
        config.expiryColumn(), 
        config.generationColumn(), 
        config.ttlColumn())
    
    val fieldNames = schema.fields.map { field => field.name}.toSet
    val binsOnly = fieldNames.toSet.diff(metaFields).toSeq.sortWith(_ < _)

	  val hasUpdateByKey = config.get(AerospikeConfig.UpdateByKey) != null
		val hasUpdateByDigest = config.get(AerospikeConfig.UpdateByDigest) != null

    if(hasUpdateByDigest && hasUpdateByKey){
      sys.error("Cannot use hasUpdateByKey and hasUpdateByDigest configuration together")
    }
	  
		val policy = writePolicy(mode, config)

		logInfo("fetch client to save partition")
		val client = AerospikeConnection.getClient(config)
		var counter = 0
		while (iterator.hasNext) {
			val row = iterator.next()
			// save row
			
			var key: Key = null
			if (hasUpdateByDigest) {
			  val digest = row(schema.fieldIndex(config.digestColumn)).asInstanceOf[Array[Byte]]
			  key = new Key(config.namespace(), digest, null, null)
			} else {
			  val keyObject: Object = row(schema.fieldIndex(config.keyColumn)).asInstanceOf[Object]
			  key = new Key(config.namespace(), config.set(), Value.get(keyObject))
			}
			
			var bins = ListBuffer[Bin]()
			binsOnly.foreach { binName => 
			  val bin = TypeConverter.fieldToBin(schema, row, binName)
			  bins += bin
			}
			
			client.put(policy, key, bins.toArray:_*)
				
			counter += 1;         
		}
		logInfo(s"Completed writing partition of $counter rows")

	}
	
}