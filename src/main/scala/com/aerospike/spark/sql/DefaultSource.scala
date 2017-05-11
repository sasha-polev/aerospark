package com.aerospike.spark.sql

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.types.StructType

import com.aerospike.client.AerospikeException
import com.aerospike.client.Key
import com.aerospike.client.ResultCode
import com.aerospike.client.Value
import com.aerospike.client.policy.GenerationPolicy
import com.aerospike.client.policy.RecordExistsAction
import com.aerospike.client.policy.WritePolicy
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.concurrent.TimeUnit

/**
  * This class provides implementations to the Spark load and save functions
  */
class DefaultSource extends RelationProvider with Serializable with LazyLogging with CreatableRelationProvider{

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val configMap = parameters ++ sqlContext.getAllConfs.map{ case (k,v) => k -> ( parameters.getOrElse(k,v)) }
    configMap.getOrElse(AerospikeConfig.SeedHost, sys.error(AerospikeConfig.SeedHost + " must be specified"))
    configMap.getOrElse(AerospikeConfig.Port, sys.error(AerospikeConfig.Port + " must be specified"))
    configMap.getOrElse(AerospikeConfig.NameSpace, sys.error(AerospikeConfig.NameSpace + " must be specified"))
    new AerospikeRelation(AerospikeConfig.newConfig(configMap), null)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val configMap = parameters ++ sqlContext.getAllConfs.map{ case (k,v) => k -> ( parameters.getOrElse(k,v)) }

    val conf = AerospikeConfig.newConfig(configMap)
    saveDataFrame(data, mode, conf)
    createRelation(sqlContext, parameters)
  }

  def saveDataFrame(data: DataFrame, mode: SaveMode, config: AerospikeConfig){
    val schema = data.schema
    data.foreachPartition { iterator =>
      savePartition(iterator, schema, mode, config) }
  }

  private def savePartition(iterator: Iterator[Row],
    schema: StructType, mode: SaveMode, config: AerospikeConfig): Unit = {
    
    val metaFields = Set(
      config.keyColumn(),
      config.digestColumn(),
      config.expiryColumn(),
      config.generationColumn(),
      config.ttlColumn()
    )

    val fieldNames = schema.fields.map { field => field.name}.toSet
    val binsOnly = fieldNames.diff(metaFields).toSeq.sortWith(_ < _)

    val hasUpdateByKey = config.get(AerospikeConfig.UpdateByKey) != null
    val hasUpdateByDigest = config.get(AerospikeConfig.UpdateByDigest) != null

    if(hasUpdateByDigest && hasUpdateByKey){
      sys.error("Cannot use hasUpdateByKey and hasUpdateByDigest configuration together")
    }
    val pool = java.util.concurrent.Executors.newFixedThreadPool(config.get(AerospikeConfig.MaxThreadCount).toString.toInt)
    logger.debug("fetch client to save partition")
    val client = AerospikeConnection.getClient(config)

    logger.debug("creating write policy")

    val policy = new WritePolicy(client.writePolicyDefault)
    policy.retryOnTimeout = true
    mode match {
      case SaveMode.ErrorIfExists => policy.recordExistsAction = RecordExistsAction.CREATE_ONLY
      case SaveMode.Ignore => policy.recordExistsAction = RecordExistsAction.CREATE_ONLY
      case SaveMode.Overwrite => policy.recordExistsAction = RecordExistsAction.REPLACE
      case SaveMode.Append => policy.recordExistsAction = RecordExistsAction.UPDATE
    }

    val genPol = config.get(AerospikeConfig.generationPolicy)

    if (genPol != null) {
      policy.generationPolicy = genPol.asInstanceOf[GenerationPolicy]
    }
    val tasks = Array.empty[Future[_]]
    while (iterator.hasNext) {
      val row = iterator.next()
      val key = if (hasUpdateByDigest) {
          val digestColumn = config.get(AerospikeConfig.UpdateByDigest).toString
          val digest = row(schema.fieldIndex(digestColumn)).asInstanceOf[Array[Byte]]
          new Key(config.namespace(), digest, config.set(), null)
        } else {
          val keyColumn = config.get(AerospikeConfig.UpdateByKey).toString
          val keyObject: Object = row(schema.fieldIndex(keyColumn)).asInstanceOf[Object]
          new Key(config.namespace(), config.set(), Value.get(keyObject))
        }

      try {
        if (policy.generationPolicy == GenerationPolicy.EXPECT_GEN_EQUAL){
          policy.generation = row(schema.fieldIndex(config.generationColumn())).asInstanceOf[java.lang.Integer].intValue
        }

        if (schema.fieldNames.contains(config.ttlColumn())){
          val expIndex = schema.fieldIndex(config.ttlColumn())
          policy.expiration = row(expIndex).asInstanceOf[java.lang.Integer].intValue
        }

        val bins = binsOnly.map(binName => TypeConverter.fieldToBin(schema, row, binName))
        
        tasks:+pool.submit(
          new Runnable {
            def run{
              client.put(policy, key, bins:_*)
            }
          })
      } catch {
        case ex: AerospikeException =>
          val message = ex.getMessage
          mode match {
            case SaveMode.ErrorIfExists =>
              ex.getResultCode match {
                case ResultCode.KEY_EXISTS_ERROR =>
                  logger.debug(s"Key:$key Error:$message")
                  throw ex
                case _ =>
                  logger.error(s"Key:$key Error:$message")
              }

            case SaveMode.Ignore =>
              ex.getResultCode match {
                case ResultCode.KEY_EXISTS_ERROR =>
                  logger.debug(s"Ignoring existing Key:$key")
                case _ =>
                  logger.error(s"Key:$key Error:$message")
                  throw ex
              }

            case SaveMode.Overwrite =>
              logger.error(s"Key:$key Error:$message")
              //throw ex

            case SaveMode.Append =>
              ex.getResultCode match {
                case ResultCode.KEY_NOT_FOUND_ERROR =>
                  logger.debug(s"Ignoring missing Key:$key")
                case _ =>
                  logger.debug(s"Key:$key Error:$message")
                  throw ex
              }
          }
      }
      
    }
    pool.shutdown()
    pool.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS);
    for(r <-tasks){
      r.onComplete {
        case Success(v) => None
        case Failure(e) => throw new AerospikeException(e)
      }
    }
  }
}
