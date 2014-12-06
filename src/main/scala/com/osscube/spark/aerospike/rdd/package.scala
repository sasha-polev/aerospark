package com.osscube.spark.aerospike

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext



package object rdd {
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)



  implicit class AeroContext(sqlContext: SQLContext) {
    def aeroRDD(
                 initialHost: (String, Int),
                 select: String,
                 numPartitionsPerServerForRange : Int = 1) =
      new SparkContextFunctions(sqlContext.sparkContext).aeroSInput(initialHost, select,sqlContext, numPartitionsPerServerForRange )

  }
}
