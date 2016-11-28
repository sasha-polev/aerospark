package com.aerospike.spark.sql

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StructType
import com.aerospike.client.Value
import com.aerospike.client.query.Statement
import com.aerospike.helper.query._
import com.aerospike.helper.query.Qualifier.FilterOperation
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.MapType


case class AerospikePartition(index: Int, host: String) extends Partition

/**
  * This is an Aerospike specific RDD to contains the results
  * of a Scan or Query operation.
  *
  * NOTE: This class uses the @see com.aerospike.helper.query.QueryEngine to
  * provide multiple filters in the Aerospike server.
  *
  */
class KeyRecordRDD(
  @transient val sc: SparkContext,
  val aerospikeConfig: AerospikeConfig,
  val schema: StructType = null,
  val requiredColumns: Array[String] = null,
  val filters: Array[Filter] = null
  ) extends RDD[Row](sc, Seq.empty) with LazyLogging {

  override protected def getPartitions: Array[Partition] = {
    val client = AerospikeConnection.getClient(aerospikeConfig)
    val nodes = client.getNodes
    var count = 0
    val parts = new Array[Partition](nodes.size)
    nodes.foreach { node =>
      val name = node.getName
      parts(count) = AerospikePartition(count, name).asInstanceOf[Partition]
      count += 1
    }
    parts
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    val partHost = partition.host
    logInfo(s"Starting partition compute() for Aerospike host: $partHost")
    val stmt = new Statement()
    stmt.setNamespace(aerospikeConfig.namespace())
    stmt.setSetName(aerospikeConfig.set())

    val metaFields = TypeConverter.metaFields(aerospikeConfig)

    if (requiredColumns != null && requiredColumns.length > 0){
      val binsOnly = TypeConverter.binNamesOnly(requiredColumns, metaFields)
      logDebug(s"Bin names: $binsOnly")
      stmt.setBinNames(binsOnly: _*)
    }

    val queryEngine = AerospikeConnection.getQueryEngine(aerospikeConfig)
    val client = AerospikeConnection.getClient(aerospikeConfig)
    val node = client.getNode(partition.host)

    val kri = if (filters != null && filters.length > 0){
      val qualifiers = filters.map { phil => filterToQualifier(phil) }
      queryEngine.select(stmt, false, node, qualifiers: _*)
    } else {
      queryEngine.select(stmt, false, node)
    }

    context.addTaskCompletionListener(context => {
      logInfo(s"KeyRecordIterator closed for Aerospike host $partHost")
      kri.close()
    })
    new RowIterator(kri, schema, aerospikeConfig, requiredColumns)
  }

  private def filterToQualifier(filter: Filter) = filter match {
    case EqualTo(attribute, value) =>
      if (isList(attribute)){
        new Qualifier(attribute, FilterOperation.LIST_CONTAINS, Value.get(value))  // TODO experimental
      } else if (isMap(attribute)){
        new Qualifier(attribute, FilterOperation.MAP_KEYS_CONTAINS, Value.get(value)) //TODO experimental
      } else {
        new Qualifier(attribute, FilterOperation.EQ, Value.get(value))
      }
    case GreaterThanOrEqual(attribute, value) =>
      new Qualifier(attribute, FilterOperation.GTEQ, Value.get(value))

    case GreaterThan(attribute, value) =>
      new Qualifier(attribute, FilterOperation.GT, Value.get(value))

    case LessThanOrEqual(attribute, value) =>
      new Qualifier(attribute, FilterOperation.LTEQ, Value.get(value))

    case LessThan(attribute, value) =>
      new Qualifier(attribute, FilterOperation.LT, Value.get(value))

    case StringStartsWith(attribute, value) =>
      new Qualifier(attribute, FilterOperation.START_WITH, Value.get(value))

    case StringEndsWith(attribute, value) =>
      new Qualifier(attribute, FilterOperation.ENDS_WITH, Value.get(value))

    case IsNotNull(attribute) =>
      new Qualifier(attribute, FilterOperation.NOTEQ, Value.getAsNull)

    case _ =>
      logger.debug(s"Not matching filter: ${filter.toString}")
      null
  }

  private def isMap(attribute: String) = {
    schema(attribute).dataType match {
      case _: MapType => true
      case _ => false
    }
  }
  private def isList(attribute: String) = {
    schema(attribute).dataType match {
      case _: ArrayType => true
      case _ => false
    }
  }
}
/**
  * This class implement a Spark SQL row iterator.
  * It is used to iterate through the Record/Result set from the Aerospike query
  */
class RowIterator[Row] (val kri: KeyRecordIterator, schema: StructType, config: AerospikeConfig, requiredColumns: Array[String] = null)
  extends Iterator[org.apache.spark.sql.Row] with LazyLogging {


  def hasNext: Boolean = {
    kri.hasNext
  }

  def next: org.apache.spark.sql.Row = {
    val kr = kri.next()

    val digest: Array[Byte] = kr.key.digest
    val digestName: String = config.digestColumn()

    val userKey: Value = kr.key.userKey
    val userKeyName: String = config.keyColumn()

    val expiration: Int = kr.record.expiration
    val expirationName: String = config.expiryColumn()

    val generation: Int = kr.record.generation
    val generationName: String = config.generationColumn()

    val ttl: Int = kr.record.getTimeToLive
    val ttlName: String = config.ttlColumn()

    val fields = requiredColumns.map { field =>
      val value = field match {
        case x if x.equals(digestName) => digest
        case x if x.equals(userKeyName) => userKey
        case x if x.equals(expirationName) => expiration
        case x if x.equals(generationName) => generation
        case x if x.equals(ttlName) => ttl
        case _ => TypeConverter.binToValue(schema, (field, kr.record.bins.get(field)))
      }
      logger.debug(s"$field = $value")
      value
    }
    Row.fromSeq(fields)
  }
}
