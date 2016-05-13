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


class AerospikeRelation( config: AerospikeConfig, userSchema: StructType)
  (@transient val sqlContext: SQLContext) 
    extends BaseRelation
    with TableScan
    with PrunedFilteredScan 
    with Logging 
    with Serializable {

  val conf = config
  
  var schemaCache: StructType = null

  override def schema: StructType = {
    val SCAN_COUNT = 10
    
    if (schemaCache == null || schemaCache.isEmpty) {

      val client = new AerospikeClient("127.0.0.1",3000)//AerospikeConnection.getClient(config)
      
      var fields = collection.mutable.Map[String,StructField]()
  		fields += "namespace" -> StructField("namespace", StringType, false)
  		fields += "set" -> StructField("set", StringType, true)
  		fields += "digest" -> StructField("digest", BinaryType, false)
  		fields += "key" -> StructField("key", StringType, true)
     
  		var stmt = new Statement();
  		stmt.setNamespace(config.get(AerospikeConfig.NameSpace).asInstanceOf[String]);
  		stmt.setSetName(config.get(AerospikeConfig.SetName).asInstanceOf[String]);
//  		val binAny = config.getIfNotEmpty(AerospikeConfig.BinList, null)
//  		if (binAny != null){
//  		  val binString = binAny.asInstanceOf[String]
//  		  val binNames = binString.split(",")
//  		  stmt.setBinNames(binNames:_*);
//  		}
  		println("***** about to query *****")
  		var recordSet = client.query(null, stmt)
  		println("***** process results *****")
  		    
  		try{
  		  val sample = recordSet.take(SCAN_COUNT)
  			sample.foreach { keyRecord => 
  			  
       /*
       BooleanType -> java.lang.Boolean
       ByteType -> java.lang.Byte
       ShortType -> java.lang.Short
       IntegerType -> java.lang.Integer
       FloatType -> java.lang.Float
       DoubleType -> java.lang.Double
       StringType -> String
       DecimalType -> java.math.BigDecimal
    
       DateType -> java.sql.Date
       TimestampType -> java.sql.Timestamp
    
       BinaryType -> byte array
       ArrayType -> scala.collection.Seq (use getList for java.util.List)
       MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
       StructType -> org.apache.spark.sql.Row
       */
    
    
        keyRecord.record.bins.foreach { bin =>
          val binVal = bin._2
          val binName = bin._1
          val field = binVal match {
            case _: java.lang.Long => StructField(binName, LongType, nullable = true)
            case _: java.lang.Double => StructField(binName, DoubleType, nullable = true)
            case s:String => StructField(binName, StringType, nullable = true)
            case Map => StructField(binName, StringType, nullable = true) //TODO 
            case List => StructField(binName, StringType, nullable = true) //TODO 
            //case ParticleType.GEOJSON => StructField(binName, StringType, nullable = true) //TODO 
            case Array => StructField(binName, BinaryType, nullable = true)
            case _ => StructField(binName, BinaryType, nullable = true)
            } 
          
            fields.get(binName) match {
              case Some(e) => fields.update(binName, field)
              case None    => fields += binName -> field
            }
          }
  			}
  		} finally {
  			recordSet.close();
  		}
  		
  		val fieldSeq = fields.values.toSeq
  		schemaCache = StructType(fieldSeq)
  	} 
    schemaCache
  }
  
  override def buildScan(): RDD[Row] = {
    new KeyRecordRDD(sqlContext.sparkContext, conf)
  }
  
  
  override def buildScan(
    requiredColumns: Array[String],
    filters: Array[Filter]): RDD[Row] = {
      
      val allFilters = filters.map { _ match {
        case EqualTo(attribute, value) =>
          new Qualifier(attribute, FilterOperation.EQ, Value.get(value))

        case GreaterThanOrEqual(attribute, value) =>
          new Qualifier(attribute, FilterOperation.GTEQ, Value.get(value))

        case GreaterThan(attribute, value) =>
          new Qualifier(attribute, FilterOperation.GT, Value.get(value))

        case LessThanOrEqual(attribute, value) =>
          new Qualifier(attribute, FilterOperation.LTEQ, Value.get(value))

        case LessThan(attribute, value) =>
          new Qualifier(attribute, FilterOperation.LT, Value.get(value))

        case _ => None
        }
      }.asInstanceOf[Array[Qualifier]]
      
      new KeyRecordRDD(sqlContext.sparkContext, conf, requiredColumns, allFilters)
      
  }
  
  
}
