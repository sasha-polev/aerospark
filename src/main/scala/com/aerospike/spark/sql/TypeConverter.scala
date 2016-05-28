package com.aerospike.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.aerospike.client.Bin

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
		  case _: LongType => value.asInstanceOf[java.lang.Long]
		  case _: IntegerType => value.asInstanceOf[java.lang.Integer]
		  case _: DoubleType => value.asInstanceOf[java.lang.Double]
		  case _ => value.toString()
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
					case s:String => StructField(binName, StringType, nullable = true)
//					case Map => StructField(binName, StringType, nullable = true) //TODO 
					case Map => StructField(binName, new MapType(), nullable = true) 
					case List => StructField(binName, StringType, nullable = true) //TODO 
					//case ParticleType.GEOJSON => StructField(binName, StringType, nullable = true) //TODO 
					case Array => StructField(binName, BinaryType, nullable = true)
					case _ => StructField(binName, BinaryType, nullable = true)
			} 
			field
	}
}


			/*
       ArrayType -> scala.collection.Seq (use getList for java.util.List)
       MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
			 */
