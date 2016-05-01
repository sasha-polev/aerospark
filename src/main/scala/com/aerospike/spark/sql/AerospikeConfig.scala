package com.aerospike.spark.sql

import collection.mutable.HashMap


class AerospikeConfig(val properties: Map[String, Any]) extends Serializable {
	private val props = properties

	def get(key: String): Any =
    properties.get(key.toLowerCase()).getOrElse(notFound(key))

  def getIfNotEmpty(key: String, defaultVal: Any): Any = {
    val proposed = properties.get(key.toLowerCase()).getOrElse(defaultVal)
    match {
      case n: Number => n.longValue
      case b: Boolean => if(b) 1 else 0
      case s: String  => if (s.isEmpty()) defaultVal else s 
      case _ => null
    }
  }
	
	def namespace(): String = {
	  get(AerospikeConfig.NameSpace).asInstanceOf[String]
	}

	def set(): String = {
	  get(AerospikeConfig.SetName).asInstanceOf[String]
	}


  private def notFound[T](key: String): T =
    throw new IllegalStateException(s"Config item $key not specified")

}

object AerospikeConfig {
	private val defaultValues = new HashMap[String, Any]
	private val defaultRequired = List(SeedHost, Port)

	final val DEFAULT_READ_PURPOSE = "spark_read"
	final val DEFAULT_WRITE_PURPOSE = "spark_write"

	val SeedHost = "aerospike.seedHost"
	defineProperty(SeedHost, null)

	val Port = "aerospike.port"
	defineProperty(Port, 3000)

	val TimeOut = "aerospike.timeout"
	defineProperty(TimeOut, 1000)
	
	val NameSpace = "aerospike.namespace"
	defineProperty(NameSpace, null)
	
	val SetName = "aerospike.set"
	defineProperty(SetName, null)
	
	val UpdateByKey = "aerospike.key"
	defineProperty(UpdateByKey, null)
	
  def apply(props: Map[String, String], required: List[String] = defaultRequired) = {
      
    val ciProps = props.map(kv => kv.copy(_1 = kv._1.toLowerCase))
    val ciRequired = required.map(x => x.toLowerCase)
    
    ciProps.keys.filter(_.startsWith("aerospike.")).foreach { x =>
      if(!defaultValues.contains(x))
        sys.error(s"Unknown Aerospike specific option : $x")
    }
  
    // Check for required properties
    require(
    ciRequired.forall(ciProps.isDefinedAt),
    s"Not all required properties are defined ! : ${
      required.diff(
        props.keys.toList.intersect(required))
    }")
    new AerospikeConfig(defaultValues.toMap ++ ciProps)
  }


	private def defineProperty(key: String, defaultValue: Any) : Unit = {
		val lowerKey = key.toLowerCase()
		if(defaultValues.contains(lowerKey))
				sys.error(s"Config property already defined for key : $key")
		else
			defaultValues.put(lowerKey, defaultValue)
	}

}