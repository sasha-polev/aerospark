package com.aerospike.spark.sql

import scala.collection.mutable.Map

class AerospikeConfig(val properties: Map[String, Any]) extends Serializable {

	def this(seedHost: String, port: String) {
    this( Map(AerospikeConfig.SeedHost -> seedHost, AerospikeConfig.Port -> port.toInt) )
  }
	
	def this(seedHost: String, port: Int) {
    this( Map(AerospikeConfig.SeedHost -> seedHost, AerospikeConfig.Port -> port) )
  }
	
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

	override def toString(): String = {
	  var buff = new StringBuffer("[")
	  properties.map(f => {
	    buff.append("{")
	    buff.append(f._1)
	    buff.append("=")
	    buff.append(f._2)
	    buff.append("}")
	  })
	  buff.append("]")
	  buff.toString()
	}
  private def notFound[T](key: String): T =
    throw new IllegalStateException(s"Config item $key not specified")

}

object AerospikeConfig {
	private val defaultValues = scala.collection.mutable.Map[String, Any](AerospikeConfig.SeedHost -> "127.0.0.1", AerospikeConfig.Port -> 3000)
	private val defaultRequired = List(SeedHost, Port)

	final val DEFAULT_READ_PURPOSE = "spark_read"
	final val DEFAULT_WRITE_PURPOSE = "spark_write"

	val SeedHost = "aerospike.seedhost"
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
	
  def apply(props: Map[String, Any], required: List[String] = defaultRequired) = {
      
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
    new AerospikeConfig(ciProps)
  }


	private def defineProperty(key: String, defaultValue: Any) : Unit = {
		val lowerKey = key.toLowerCase()
		if(defaultValues.contains(lowerKey))
				sys.error(s"Config property already defined for key : $key")
		else
			defaultValues.put(lowerKey, defaultValue)
	}

}