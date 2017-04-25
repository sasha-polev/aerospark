package com.aerospike

import org.apache.spark.sql.Dataset

package object spark {
  
  implicit def toDatasetFunctions[T](dataset: Dataset[T]): DatasetFunctions[T] = new DatasetFunctions(dataset)
}