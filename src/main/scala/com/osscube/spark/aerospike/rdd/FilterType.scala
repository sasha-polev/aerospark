package com.osscube.spark.aerospike.rdd

sealed trait FilterType

sealed trait AeroFilterType extends FilterType

sealed trait SparkFilterType extends FilterType

case object FilterNone extends AeroFilterType with SparkFilterType

case object FilterString extends AeroFilterType with SparkFilterType

case object FilterLong extends AeroFilterType with SparkFilterType

case object FilterRange extends AeroFilterType with SparkFilterType

case object FilterIn extends SparkFilterType

