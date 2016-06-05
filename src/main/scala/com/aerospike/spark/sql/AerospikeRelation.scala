package com.aerospike.spark.sql

import scala.collection.JavaConversions._
import scala.util.parsing.json.JSONType

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.MapType

import com.aerospike.client.Value
import com.aerospike.client.command.ParticleType
import com.aerospike.helper.query.Qualifier
import com.aerospike.helper.query.Qualifier.FilterOperation
import com.aerospike.spark.sql.AerospikeConfig._
import com.aerospike.helper.query.KeyRecordIterator
import com.aerospike.client.query.KeyRecord
import com.aerospike.client.query.Statement
import com.aerospike.client.policy.QueryPolicy
import com.aerospike.client.AerospikeClient
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.IntegerType
import scala.collection.immutable.ListMap

/**
 * This class infers the schema used by the DataFrame
 * and creates an instance of @see com.aerospike.spark.sql.KeyRecordRDD
 */
class AerospikeRelation( config: AerospikeConfig, userSchema: StructType)
  (@transient val sqlContext: SQLContext) 
    extends BaseRelation
    with TableScan
    with PrunedFilteredScan 
    with Logging 
    with Serializable {
  
  Value.UseDoubleType = true

  val conf = config
  
  var schemaCache: StructType = null

  override def schema: StructType = {
    val SCAN_COUNT = config.schemaScan()
    
    if (schemaCache == null || schemaCache.isEmpty) {

      val client = AerospikeConnection.getClient(config) 
      
      var bins = collection.mutable.Map[String,StructField]()
      var fields = ListBuffer[StructField]()
  		fields += StructField(config.keyColumn(), StringType, true)
  		fields += StructField(config.digestColumn(), BinaryType, false)
  		fields += StructField(config.expiryColumn(), IntegerType, false)
  		fields += StructField(config.generationColumn(), IntegerType, false)
  		fields += StructField(config.ttlColumn(), IntegerType, false)
     
  		var stmt = new Statement();
  		stmt.setNamespace(config.get(AerospikeConfig.NameSpace).asInstanceOf[String]);
  		stmt.setSetName(config.get(AerospikeConfig.SetName).asInstanceOf[String]);
  		var recordSet = client.query(null, stmt)
  		    
  		try{
  		  val sample = recordSet.take(SCAN_COUNT)
  			sample.foreach { keyRecord => 
    
          keyRecord.record.bins.foreach { bin =>
            val binVal = bin._2
            val binName = bin._1
            val field = TypeConverter.valueToSchema(bin)
            //println(s"Bin:$binName, Value:$binVal, Field:$field")
              bins.get(binName) match {
                case Some(e) => bins.update(binName, field)
                case None    => bins += binName -> field
              }
            }
  			}
  		} finally {
  			recordSet.close();
  		}
  		// sort fields by bin name
  		bins.toSeq.sortBy(_._1).map(bin => fields += bin._2)
  		
  		val fieldSeq = fields.toSeq
  		schemaCache = StructType(fieldSeq)
  	} 
    schemaCache
  }
  
  override def buildScan(): RDD[Row] = {
    new KeyRecordRDD(sqlContext.sparkContext, conf, schemaCache)
  }
  
  
  override def buildScan(
    requiredColumns: Array[String],
    filters: Array[Filter]): RDD[Row] = {
    if (filters.length >0){
      new KeyRecordRDD(sqlContext.sparkContext, conf, schemaCache, requiredColumns, filters)
    } else {
      new KeyRecordRDD(sqlContext.sparkContext, conf, schemaCache, requiredColumns)
    }
  }
  
  
}
