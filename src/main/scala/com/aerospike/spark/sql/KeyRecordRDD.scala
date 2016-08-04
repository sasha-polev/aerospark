package com.aerospike.spark.sql

import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.GreaterThan
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.apache.spark.sql.sources.LessThan
import org.apache.spark.sql.sources.LessThanOrEqual
import org.apache.spark.sql.sources.StringStartsWith
import org.apache.spark.sql.sources.StringEndsWith

import org.apache.spark.sql.types.StructType

import com.aerospike.client.Value
import com.aerospike.client.cluster.Node
import com.aerospike.client.query.Statement
import com.aerospike.helper.query._
import com.aerospike.helper.query.Qualifier.FilterOperation
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.MapType


case class AerospikePartition(index: Int,
                              host: String) extends Partition()

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
                    ) extends RDD[Row](sc, Seq.empty) with Logging {  
  

  override protected def getPartitions: Array[Partition] = {
    {
      var client = AerospikeConnection.getClient(aerospikeConfig) 
      var nodes = client.getNodes
      for {i <- 0 until nodes.size
           node: Node = nodes(i)
           val name = node.getName
           part = new AerospikePartition(i, name).asInstanceOf[Partition]
      } yield part
    }.toArray
  }
  
  
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    logDebug("Starting compute() for partition: " + partition.index)
    val stmt = new Statement()
    stmt.setNamespace(aerospikeConfig.namespace())
    stmt.setSetName(aerospikeConfig.set())

    val metaFields = TypeConverter.metaFields(aerospikeConfig)

    if (requiredColumns != null && requiredColumns.length > 0){
      val binsOnly = TypeConverter.binNamesOnly(requiredColumns, metaFields)
      stmt.setBinNames(binsOnly: _*) 
    }
    
    val queryEngine = AerospikeConnection.getQueryEngine(aerospikeConfig)
    val client = AerospikeConnection.getClient(aerospikeConfig)
    val node = client.getNode(partition.host);
    
    var kri: KeyRecordIterator = null
    
    if (filters != null && filters.length > 0){
      val qualifiers = filters.map { phil => filterToQualifier(phil) }
      kri = queryEngine.select(stmt, false, node, qualifiers: _*)
    } else {
      kri = queryEngine.select(stmt, false, node)
    }

    context.addTaskCompletionListener(context => { kri.close() })
    
    
    new RowIterator(kri, schema, metaFields)
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
          
      case _ => null            
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
class RowIterator[Row] (val kri: KeyRecordIterator, schema: StructType, metaFields: Set[String]) extends Iterator[org.apache.spark.sql.Row] with Logging{
     
      def hasNext: Boolean = {
        kri.hasNext()
      }
     
      def next: org.apache.spark.sql.Row = {
         val kr = kri.next()
         // close if nothing left
         //if (!kri.hasNext()) logInfo("*******nothing left")
         //logInfo(s"Read: $kr")
         val expiration: Int = kr.record.expiration
         val generation: Int = kr.record.generation
         val ttl: Int = kr.record.getTimeToLive
         var fields = scala.collection.mutable.MutableList[Any](
            kr.key.userKey,
            kr.key.digest,
            expiration,
            generation,
            ttl
            )

        val fieldNames = schema.fields.map { field => field.name}.toSet
         
        // remove the meta data and sort the bins by names
        val binsOnly = TypeConverter.binNamesOnly(fieldNames.toArray, metaFields)
          
        binsOnly.foreach { field => 
          val value = TypeConverter.binToValue(schema, (field, kr.record.bins.get(field)))
          fields += value
        }
        
        val row = Row.fromSeq(fields.toSeq)
        row
      }
}

