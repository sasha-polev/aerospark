package com.aerospike.spark.sql

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import com.aerospike.client.Bin

object TypeConverter {

  def binToValue(schema: StructType, bin: (String, Object)): Any = {
    
		val binVal = bin._2
		val binName = bin._1
    val value = schema(binName).dataType match {
		  case _: LongType => binVal.asInstanceOf[java.lang.Long].longValue
		  case _: IntegerType => binVal.asInstanceOf[java.lang.Integer].intValue
		  case _: DoubleType => binVal.asInstanceOf[java.lang.Double].doubleValue
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
					case _: java.lang.Long => StructField(binName, LongType, nullable = true)
					case _: java.lang.Double => StructField(binName, DoubleType, nullable = true)
					case s:String => StructField(binName, StringType, nullable = true)
					case Map => StructField(binName, StringType, nullable = true) //TODO 
					case List => StructField(binName, StringType, nullable = true) //TODO 
					//case ParticleType.GEOJSON => StructField(binName, StringType, nullable = true) //TODO 
					case Array => StructField(binName, BinaryType, nullable = true)
					case _ => StructField(binName, BinaryType, nullable = true)
			} 
			field
	}
}


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
