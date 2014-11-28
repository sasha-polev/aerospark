package com.osscube.spark.aerospike

import org.apache.spark.SparkContext


package object rdd {
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)
}
