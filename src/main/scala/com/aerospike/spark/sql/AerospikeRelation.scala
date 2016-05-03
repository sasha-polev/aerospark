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


case class AerospikeRelation(
  config: AerospikeConfig)
  (@transient val sqlContext: SQLContext) extends BaseRelation
    with PrunedFilteredScan with Logging{

  @transient val queryEngine = AerospikeConnection.getQueryEngine(config)

  var schemaCache: StructType = null

  override def schema: StructType = {

    if (schemaCache.isEmpty) {
      val columnTypes = mapAsScalaMap(queryEngine.inferSchema(config.get(AerospikeConfig.NameSpace).toString(),
          config.get(AerospikeConfig.SetName).toString(), 100))
          
    for ((k,v) <- columnTypes) printf("key: %s, value: %s\n", k, v)
     
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
    
    
      val schemaFields = columnTypes.map { b =>
        b._2.intValue() match {
          case ParticleType.INTEGER => StructField(b._1, LongType, nullable = true)
          case ParticleType.DOUBLE => StructField(b._1, DoubleType, nullable = true)
          case ParticleType.STRING => StructField(b._1, StringType, nullable = true)
          case ParticleType.MAP => StructField(b._1, StringType, nullable = true) //TODO 
          case ParticleType.LIST => StructField(b._1, StringType, nullable = true) //TODO 
          case ParticleType.GEOJSON => StructField(b._1, StringType, nullable = true) //TODO 
          case ParticleType.BLOB => StructField(b._1, BinaryType, nullable = true)
          case _ => StructField(b._1, BinaryType, nullable = true)
        }       
      }
      schemaCache = StructType(schemaFields.toArray)
    }
    schemaCache
  }
  
  
  override def buildScan(
    requiredColumns: Array[String],
    filters: Array[Filter]): RDD[Row] = {
    
    if (filters.length > 0) {
      
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
      
      new AerospikeRDD(sqlContext.sparkContext, config, requiredColumns, allFilters)
      
    } else {
      new AerospikeRDD(sqlContext.sparkContext, config, requiredColumns)
    }
  }
}
