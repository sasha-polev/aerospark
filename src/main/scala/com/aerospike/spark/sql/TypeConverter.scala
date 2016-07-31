package com.aerospike.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.aerospike.client.Bin
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DateType

/**
 * This object provides utility methods to convert between
 * the Aerospike and park SQL types
 */
object TypeConverter{
  
  def binNamesOnly(fieldNames:Array[String], metaFields:Set[String]):Array[String] = {
    val binsOnly = fieldNames.toSet.diff(metaFields).toSeq.sortWith(_ < _)
    binsOnly.toArray
  }

  def metaFields(aerospikeConfig: AerospikeConfig): Set[String] = { 
    Set(aerospikeConfig.keyColumn(),
        aerospikeConfig.digestColumn(), 
        aerospikeConfig.expiryColumn(), 
        aerospikeConfig.generationColumn(), 
        aerospikeConfig.ttlColumn())
  }
  
  def binToValue(schema: StructType, bin: (String, Object)): Any = {
    
		val binVal = bin._2
		val binName = bin._1
    val value = schema(binName).dataType match {
		  case _: LongType => binVal.asInstanceOf[java.lang.Number].longValue
		  case _: IntegerType => binVal.asInstanceOf[java.lang.Number].intValue
		  case _: DoubleType => binVal.asInstanceOf[java.lang.Number].doubleValue()
		  case _ => binVal.toString()
		}
    value
  }
  
  def fieldToBin(schema: StructType, row:Row, field:String): Bin = {
    
		val value = row(schema.fieldIndex(field))
    val binValue = schema(field).dataType match {
		  case LongType => if (value == null) null else value.asInstanceOf[java.lang.Long]
		  case IntegerType => if (value == null) null else new java.lang.Long(value.asInstanceOf[Int])
		  case ShortType => if (value == null) null else value.asInstanceOf[java.lang.Long]
		  case DoubleType => if (value == null) null else value.asInstanceOf[java.lang.Double]
		  case FloatType => if (value == null) null else value.asInstanceOf[java.lang.Double]
		  case DateType => if (value == null) null else value.asInstanceOf[java.sql.Date].getTime
		  case _: ArrayType => value
		  case _: MapType => value
		  case null => null
		  case _ => if (value == null) null else value.toString()
		}
    new Bin(field, binValue)
  }
  
	def valueToSchema(bin: (String, Object)): StructField = {

			val binVal = bin._2
			val binName = bin._1
			val field = binVal match {
					case _: java.lang.Integer => StructField(binName, LongType, nullable = true)
					case _: java.lang.Short => StructField(binName, LongType, nullable = true)
					case _: java.lang.Long => StructField(binName, LongType, nullable = true)
					case _: java.lang.Double => StructField(binName, DoubleType, nullable = true)
					case _: java.lang.Float => StructField(binName, DoubleType, nullable = true)
					case s: String => StructField(binName, StringType, nullable = true)
					case Map => {
					  val aKey = valueToSchema((binName, binVal.asInstanceOf[Map[Object, Object]].keys.head))
					  val aValue = valueToSchema((binName, binVal.asInstanceOf[Map[Object, Object]].values.head))
					  StructField(binName, new MapType(aKey.dataType, aValue.dataType, true), nullable = true) 
					}
					case List => { 
					  val newValue = binVal.asInstanceOf[java.util.List[Object]].get(0)
					  val elementStructure = valueToSchema((binName, newValue))
					  StructField(binName, new ArrayType(elementStructure.dataType , true), nullable = true) 
					}
					//case ParticleType.GEOJSON => StructField(binName, StringType, nullable = true) //TODO 
					case Array => StructField(binName, BinaryType, nullable = true)
					case _ => StructField(binName, BinaryType, nullable = true)
			} 
			field
	}
	
}
