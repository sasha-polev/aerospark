package com.aerospike.spark.sql

import org.scalatest.FlatSpec

class ConfigTest extends FlatSpec {
  behavior of "Aerospike Configuration"
  
  it should " create defaults" in {
    val conf = AerospikeConfig.apply()
    
    assert(conf.port() == 3000)
    assert(conf.seedHost() == "127.0.0.1")
    assert(conf.schemaScan() == 100)
    assert(conf.namespace() == "test")
    assert(conf.keyColumn() == "__key")
    assert(conf.digestColumn() == "__digest")
    assert(conf.expiryColumn() == "__expiry")
    assert(conf.generationColumn() == "__generation")
    assert(conf.ttlColumn() == "__ttl")
  }
  
  it should " create defaults with overrides" in {
    val parameters = Map(AerospikeConfig.SeedHost -> "192.168.0.1", 
        AerospikeConfig.Port -> 4000,
        AerospikeConfig.KeyColumn -> "_my_key") 
    val conf = AerospikeConfig.apply(parameters)

    assert(conf.port() == 4000)
    assert(conf.seedHost() == "192.168.0.1")
    assert(conf.namespace() == "test")
    assert(conf.keyColumn() == "_my_key")
    assert(conf.digestColumn() == "__digest")
    assert(conf.expiryColumn() == "__expiry")
    assert(conf.generationColumn() == "__generation")
    assert(conf.ttlColumn() == "__ttl")
    assert(conf.schemaScan() == 100)
  }
}