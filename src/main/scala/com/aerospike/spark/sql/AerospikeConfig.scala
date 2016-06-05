package com.aerospike.spark.sql

import scala.collection.immutable.Map
import com.aerospike.client.policy.CommitLevel
import com.aerospike.client.policy.GenerationPolicy
/**
 * this class is a container for the properties used during the
 * the read and save functions 
 */
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

	def seedHost(): String = {
	  get(AerospikeConfig.SeedHost).asInstanceOf[String]
	}

	def port(): Int = {
	  get(AerospikeConfig.Port).asInstanceOf[Int]
	}
	
	def schemaScan(): Int = {
	  get(AerospikeConfig.SchemaScan).asInstanceOf[Int]
	}
	
	def keyColumn(): String = {
	  get(AerospikeConfig.KeyColumn).asInstanceOf[String]
	}
	
	def digestColumn(): String = {
	  get(AerospikeConfig.DigestColumn).asInstanceOf[String]
	}
	
	def expiryColumn(): String = {
	  get(AerospikeConfig.ExpiryColumn).asInstanceOf[String]
	}
	
	def generationColumn(): String = {
	  get(AerospikeConfig.GenerationColumn).asInstanceOf[String]
	}
	
	def ttlColumn(): String = {
	  get(AerospikeConfig.TTLColumn).asInstanceOf[String]
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
	private val defaultValues = scala.collection.mutable.Map[String, Any](
	        AerospikeConfig.SeedHost -> "127.0.0.1", 
	        AerospikeConfig.Port -> 3000,
	        AerospikeConfig.SchemaScan -> 100,
	        AerospikeConfig.NameSpace -> "test",
	        AerospikeConfig.KeyColumn -> "__key",
	        AerospikeConfig.DigestColumn -> "__digest",
	        AerospikeConfig.ExpiryColumn -> "__expiry",
	        AerospikeConfig.GenerationColumn -> "__generation",
	        AerospikeConfig.TTLColumn -> "__ttl")
	        
	final val DEFAULT_READ_PURPOSE = "spark_read"
	final val DEFAULT_WRITE_PURPOSE = "spark_write"

	val SeedHost = "aerospike.seedhost"
	defineProperty(SeedHost, "127.0.0.1")

	val Port = "aerospike.port"
	defineProperty(Port, 3000)

	val TimeOut = "aerospike.timeout"
	defineProperty(TimeOut, 0)
	
	val sendKey = "aerospike.sendKey"
	defineProperty(sendKey, false)
	
	val commitLevel = "aerospike.commitLevel"
	defineProperty(commitLevel, CommitLevel.COMMIT_ALL)
	
	val generationPolicy = "aerospike.generationPolicy"
	defineProperty(generationPolicy, GenerationPolicy.NONE)
	
	val NameSpace = "aerospike.namespace"
	defineProperty(NameSpace, "test")
	
	val SetName = "aerospike.set"
	defineProperty(SetName, null)
	
	val UpdateByKey = "aerospike.updateByKey"
	defineProperty(UpdateByKey, null)
	
	val UpdateByDigest = "aerospike.updateByDigest"
	defineProperty(UpdateByDigest, null)
	
	val SchemaScan = "aerospike.schema.scan"
	defineProperty(SchemaScan, 100)
	
	val KeyColumn = "aerospike.keyColumn"
	defineProperty(KeyColumn, "__key")

	val DigestColumn = "aerospike.digestColumn"
	defineProperty(DigestColumn, "__digest")

	val ExpiryColumn = "aerospike.expiryColumn"
	defineProperty(ExpiryColumn, "__expiry")

	val GenerationColumn = "aerospike.generationColumn"
	defineProperty(GenerationColumn, "__generation")

	val TTLColumn = "aerospike.ttlColumn"
	defineProperty(TTLColumn, "__ttl")

	def apply(props: Map[String, Any] = null) = {
    if (props != null) {
      val ciProps = props.map(kv => kv.copy(_1 = kv._1.toLowerCase))
      
      ciProps.keys.filter(_.startsWith("aerospike.")).foreach { x =>
        if(!defaultValues.contains(x))
          sys.error(s"Unknown Aerospike specific option : $x")
      }
      val mergedProperties = defaultValues.toMap ++ ciProps
      new AerospikeConfig(mergedProperties)
    } else {
      new AerospikeConfig(defaultValues.toMap)
    }
  }


	private def defineProperty(key: String, defaultValue: Any) : Unit = {
		val lowerKey = key.toLowerCase()
		if(defaultValues.contains(lowerKey))
				sys.error(s"Config property already defined for key : $key")
		else
			defaultValues.put(lowerKey, defaultValue)
	}
	
	def newConfig(seedHost:String, port: Int): AerospikeConfig = {
	  var conf = new AerospikeConfig(seedHost, port)
	  conf
	}
	
	def newConfig(seedHost:String, port: String): AerospikeConfig = {
	  var conf = new AerospikeConfig(seedHost, port)
	  conf
	}

}