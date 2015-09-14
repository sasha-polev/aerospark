package com.osscube.spark.aerospike

import com.aerospike.client.{AerospikeClient, Info}
import com.osscube.spark.aerospike.AerospikeUtils.resourceUDF
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class AerospikeUtilsSpec extends FlatSpec with Matchers with BeforeAndAfter {

  var client: AerospikeClient = _
  before {
    client = AerospikeInstance.aerospikeClient
  }

  after {
    client.close()
  }

  behavior of "register UDF"

  it should "do nothing when register and UDF exits" in {
    AerospikeUtils.registerUdfWhenNotExist(client)

    AerospikeUtils.registerUdfWhenNotExist(client) match {
      case None =>
      case Some(task) => fail
    }
  }

  it should "remove UDF when exists and register it" in {
    if (Info.request(client.getNodes.head, "udf-list").contains("filename=" + resourceUDF)) {
      println("removing udf")
      client.removeUdf(null, resourceUDF)
      Info.request(client.getNodes.head, "udf-list") should not include ("filename=" + resourceUDF)
    }


    AerospikeUtils.registerUdfWhenNotExist(client) match {
      case None => fail
      case Some(task) => {
        task.waitTillComplete(1000)
        task.isDone shouldBe true
        Info.request(client.getNodes.head, "udf-list") should include("filename=" + resourceUDF)
      }
    }
  }
}
