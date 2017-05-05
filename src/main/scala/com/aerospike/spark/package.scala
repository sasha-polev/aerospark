package com.aerospike

import org.apache.spark.sql.Dataset
import com.aerospike.spark.sql.AerospikeConfig
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
import scala.reflect._
import scala.reflect.runtime.universe._

package object spark {
  
  implicit def toDatasetFunctions[T](dataset: Dataset[T]): AeroSparkDatasetFunctions[T] = new AeroSparkDatasetFunctions(dataset)
  implicit def toAerospikeSessionFunctions(sparkSession: SparkSession): AeroSparkSessionFunctions = new AeroSparkSessionFunctions(sparkSession)
    
    /** 
  *  Returns a map of configuration for aesropike client connection
  *  
  * */
  def aerospikeConfs(
      set: String,
      namespace: String,
      seedHost: String = AerospikeConfig.DEFAULT_SEED_HOST): Map[String, String] =
    Map(
      AerospikeConfig.SeedHost -> seedHost,
      AerospikeConfig.NameSpace -> namespace,
      AerospikeConfig.SetName -> set)
  
  implicit class DataFrameReaderWrapper(val dfReader: DataFrameReader) extends AnyVal {
    /** Sets the format used to access Aerospike through Client */
    def aerospikeFormat: DataFrameReader = {
      dfReader.format("com.aerospike.spark.sql")
    }

    /** Sets the format used to access Cassandra through Connector and configure a path to Cassandra table. */
    def aerospikeFormat(set: String): DataFrameReader = {
      aerospikeFormat.option("aerospike.set",set)
    }
  }
  
  implicit class DataFrameWriterWrapper[T](val dfWriter: DataFrameWriter[T]) extends AnyVal {
    /** Sets the format used to access Aerospike through client */
    def aerospikeFormat: DataFrameWriter[T] = {
      dfWriter.format("com.aerospike.spark.sql")
    }

    /** Sets the format used to access Aerospike set */
    def aerospikeFormat(
       set: String,
       key: String): DataFrameWriter[T] = {

      aerospikeFormat
        .option(AerospikeConfig.SetName, set)
        .option(AerospikeConfig.UpdateByKey, key)
    }
  }
  
  /**
   * reflection Map to Object of case class
   */
  def fromMap[T: TypeTag: ClassTag](m: Map[String,_]) = {
    val rm = runtimeMirror(classTag[T].runtimeClass.getClassLoader)
    val classTest = typeOf[T].typeSymbol.asClass
    val classMirror = rm.reflectClass(classTest)
    val constructor = typeOf[T].decl(termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructor)

    val constructorArgs = constructor.paramLists.flatten.map( (param: Symbol) => {
      val paramName = param.name.toString
      if(param.typeSignature <:< typeOf[Option[Any]])
        m.get(paramName)
      else
        m.get(paramName).getOrElse(throw new IllegalArgumentException("Map is missing required parameter named " + paramName))
    })

    constructorMirror(constructorArgs:_*)
  }
  
  def typeToClassTag[T: TypeTag]: ClassTag[T] = { ClassTag[T]( typeTag[T].mirror.runtimeClass( typeTag[T].tpe )) }
  
}
