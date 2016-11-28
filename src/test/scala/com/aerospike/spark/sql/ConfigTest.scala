package com.aerospike.spark.sql

import org.scalatest.FlatSpec

class ConfigTest extends FlatSpec {
  behavior of "Aerospike Configuration"

  it should " create defaults" in {
    val conf = AerospikeConfig.newConfig()

    assert(conf.port() == 3000)
    assert(conf.seedHost() == "127.0.0.1")
    assert(conf.schemaScan() == 100)
    assert(conf.keyColumn() == "__key")
    assert(conf.digestColumn() == "__digest")
    assert(conf.expiryColumn() == "__expiry")
    assert(conf.generationColumn() == "__generation")
    assert(conf.ttlColumn() == "__ttl")
  }

  it should " create defaults with overrides" in {
    val parameters = Map(AerospikeConfig.SeedHost -> Globals.seedHost,
      AerospikeConfig.Port -> 4000,
      AerospikeConfig.NameSpace -> Globals.namespace,
      AerospikeConfig.KeyColumn -> "_my_key")
    val conf = AerospikeConfig.newConfig(parameters)

    assert(conf.port() == 4000)
    assert(conf.seedHost() == Globals.seedHost)
    assert(conf.namespace() == Globals.namespace)
    assert(conf.keyColumn() == "_my_key")
    assert(conf.digestColumn() == "__digest")
    assert(conf.expiryColumn() == "__expiry")
    assert(conf.generationColumn() == "__generation")
    assert(conf.ttlColumn() == "__ttl")
    assert(conf.schemaScan() == 100)
  }

  it should " create with timeout" in {
    val parameters = Map(AerospikeConfig.SeedHost -> Globals.seedHost,
      AerospikeConfig.Port -> 4000,
      AerospikeConfig.TimeOut -> 600,
      AerospikeConfig.NameSpace -> Globals.namespace,
      AerospikeConfig.KeyColumn -> "_my_key")
    val conf = AerospikeConfig.newConfig(parameters)

    assert(conf.port() == 4000)
    assert(conf.seedHost() == Globals.seedHost)
    assert(conf.namespace() == Globals.namespace)
    assert(conf.keyColumn() == "_my_key")
    assert(conf.digestColumn() == "__digest")
    assert(conf.expiryColumn() == "__expiry")
    assert(conf.generationColumn() == "__generation")
    assert(conf.ttlColumn() == "__ttl")
    assert(conf.schemaScan() == 100)
    assert(conf.timeOut == 600)
  }

}
