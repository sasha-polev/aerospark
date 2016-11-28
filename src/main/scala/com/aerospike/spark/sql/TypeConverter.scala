package com.aerospike.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import collection.JavaConverters._
import com.aerospike.client.Bin

import scala.collection.mutable


/**
  * This object provides utility methods to convert between
  * the Aerospike and park SQL types
  */
object TypeConverter{

  def binNamesOnly(fieldNames:Array[String], metaFields:Set[String]): Array[String] = {
    fieldNames.toSet.diff(metaFields).toArray.sortWith(_ < _)
  }

  def metaFields(aerospikeConfig: AerospikeConfig): Set[String] = {
    Set(
      aerospikeConfig.keyColumn(),
      aerospikeConfig.digestColumn(),
      aerospikeConfig.expiryColumn(),
      aerospikeConfig.generationColumn(),
      aerospikeConfig.ttlColumn())
  }

  def binToValue(schema: StructType, bin: (String, Object)): Any = {
    val (binName, binVal) = bin

    Option(schema(binName).dataType) match {
      case Some(value) =>
        value match {
          case _: LongType => Option(binVal).map(_.asInstanceOf[java.lang.Number].longValue()).orNull
          case _: IntegerType => Option(binVal).map(_.asInstanceOf[java.lang.Number].intValue()).orNull
          case _: DoubleType =>  Option(binVal).map(_.asInstanceOf[java.lang.Number].doubleValue()).orNull
          case _: ArrayType => binVal
          case _: MapType => Option(binVal).map(_.asInstanceOf[java.util.Map[Any,Any]].asScala).orNull
          case null => null
          case _ => Option(binVal).map(_.toString).orNull
        }
      case None => null
    }
  }

  def fieldToBin(schema: StructType, row:Row, field: String): Bin = {
    Option(row(schema.fieldIndex(field))) match {
      case None => Bin.asNull(field)
      case Some(value) =>
        schema(field).dataType match {
          case StringType => new Bin(field, value)
          case LongType => new Bin(field, value.asInstanceOf[java.lang.Long])
          case IntegerType => new Bin(field, new java.lang.Long(value.asInstanceOf[Int]))
          case ShortType => new Bin(field, value.asInstanceOf[java.lang.Long])
          case DoubleType => new Bin(field, value.asInstanceOf[java.lang.Double])
          case FloatType => new Bin(field, value.asInstanceOf[java.lang.Double])
          case DateType => new Bin(field, value.asInstanceOf[java.sql.Date].getTime)
          case NullType => Bin.asNull(field)
          case _: ArrayType =>
            value match {
              case _: mutable.WrappedArray[Any] => new Bin(field, value.asInstanceOf[mutable.WrappedArray[Any]].toList.asJava)
              case _: Seq[Any] => new Bin(field, scala.collection.JavaConversions.seqAsJavaList(value.asInstanceOf[Seq[Any]]))
              case _: List[Any] => new Bin(field, value)
            }
          case _: MapType =>
            value match {
              case _: scala.collection.Map[Any, Any] => new Bin(field, value.asInstanceOf[scala.collection.Map[Any, Any]].asJava)
            }
          case _ => new Bin(field, value.toString)
        }
      }
  }

  def valueToSchema(bin: (String, Object)): StructField = {
    val (binName, binVal) = bin

    binVal match {
      case _: java.lang.Integer => StructField(binName, LongType, nullable = true)
      case _: java.lang.Short => StructField(binName, LongType, nullable = true)
      case _: java.lang.Long => StructField(binName, LongType, nullable = true)
      case _: java.lang.Double => StructField(binName, DoubleType, nullable = true)
      case _: java.lang.Float => StructField(binName, DoubleType, nullable = true)
      case _: String => StructField(binName, StringType, nullable = true)
      case _: java.util.Map[Object,Object] =>
        val aKey = valueToSchema((binName, binVal.asInstanceOf[java.util.Map[Object, Object]].asScala.keys.head))
        val aValue = valueToSchema((binName, binVal.asInstanceOf[java.util.Map[Object, Object]].asScala.values.head))
        StructField(binName, new MapType(aKey.dataType, aValue.dataType, true), nullable = true)
      case _: java.util.List[Object] =>
        val newValue = binVal.asInstanceOf[java.util.List[Object]].get(0)
        val elementStructure = valueToSchema((binName, newValue))
        StructField(binName, new ArrayType(elementStructure.dataType , true))
      //case ParticleType.GEOJSON => StructField(binName, StringType, nullable = true) //TODO
      case Array => StructField(binName, BinaryType, nullable = true)
      case _ => StructField(binName, BinaryType, nullable = true)
    }
  }
}
