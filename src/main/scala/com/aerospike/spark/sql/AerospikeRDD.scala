package com.aerospike.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.Filter
import com.aerospike.helper.query._
import com.aerospike.client.query.Statement




class AerospikeRDD(
                    @transient override val sc: SparkContext,
                    @transient val aerospikeConfig: AerospikeConfig,
                    val namespace: String,
                    val set: String,
                    val bins: Seq[String],
                    val qualifiers: Array[Qualifier],
                    @transient override val filterVals: Seq[(Long, Long)]
                    ) extends RDD[Row](sc, Seq.empty) with Logging {  

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    val stmt = new Statement()
    stmt.setNamespace(namespace)
    stmt.setSetName(set)
    stmt.setBinNames(bins: _*)
    
    val queryEngine = AerospikeConnection.getQueryEngine(aerospikeConfig)
    queryEngine.select(stmt, qualifiers);
    context.addTaskCompletionListener(context => {
      res.close();
      client.close()
    })


  }

}


//Filter types: 0 none, 1 - equalsString, 2 - equalsLong, 3 - range

case class AerospikePartition(index: Int,
                              endpoint: (String, Int, String),
                              startRange: Long,
                              endRange: Long
                               ) extends Partition()
