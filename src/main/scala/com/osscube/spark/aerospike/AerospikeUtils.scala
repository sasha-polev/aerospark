package com.osscube.spark.aerospike

import com.aerospike.client.task.RegisterTask
import com.aerospike.client.{AerospikeClient, Info, Language}

object AerospikeUtils {

  def resourceUDF: String = "spark_filters.lua"

  def registerUdfWhenNotExist(client: AerospikeClient): Option[RegisterTask] = {
    var task:Option[RegisterTask] = None
    if (!Info.request(client.getNodes.head, "udf-list").contains("filename=" + resourceUDF)) {
      task = Some(client.register(null, this.getClass.getClassLoader,
        resourceUDF, resourceUDF, Language.LUA))
    }

    task
  }
}
