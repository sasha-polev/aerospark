package com.aerospike.spark.sql

import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import com.aerospike.client.cluster.Node
import com.aerospike.client.query.Statement
import com.aerospike.helper.query._


case class AerospikePartition(index: Int,
                              host: String) extends Partition()


class KeyRecordRDD(
                    @transient val sc: SparkContext,
                    val aerospikeConfig: AerospikeConfig,
                    val schema: StructType = null,
                    val requiredColumns: Array[String] = null,
                    val qualifiers: Array[Qualifier] = null
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
    //stmt.setBinNames(bins: _*) TODO
    
    val queryEngine = AerospikeConnection.getQueryEngine(aerospikeConfig)
    val client = AerospikeConnection.getClient(aerospikeConfig)
    val node = client.getNode(partition.host);
    
    val kri = queryEngine.select(stmt, false, node, qualifiers: _*)

    context.addTaskCompletionListener(context => { kri.close() })
    
    new RowIterator(kri, schema)
  }
}

class RowIterator[Row] (val kri: KeyRecordIterator, schema: StructType) extends Iterator[org.apache.spark.sql.Row] {
     
      def hasNext: Boolean = {
        kri.hasNext()
      }
     
      def next: org.apache.spark.sql.Row = {
         val kr = kri.next()
         var fields = scala.collection.mutable.MutableList[Any](
            kr.key.userKey,
            kr.key.digest
//            kr.record.expiration.toInt,
//            kr.record.generation.toInt
            )

//        val fieldNames = schema.fields.map { field => field.name}.toSet
//        
//        val binsOnly = fieldNames.diff(RowIterator.metaFields)
//          
//        binsOnly.foreach { field => 
//          val value = TypeConverter.binToValue(schema, (field, kr.record.bins.get(field)))
//          fields += value
//        }
         
        val row = Row.fromSeq(fields.toSeq)
        row
      }
}

object  RowIterator {
  val metaFields = Set("key","digest", "expiration", "generation")
}
