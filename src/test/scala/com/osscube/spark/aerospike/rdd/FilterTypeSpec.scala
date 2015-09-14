package com.osscube.spark.aerospike.rdd

import org.scalatest.{FlatSpec, Matchers}

class FilterTypeSpec extends FlatSpec with Matchers {

   behavior of "FilterType"

  it should "a sparkFilter be an aeroFilter" in {
    val sparkFilter: SparkFilterType = FilterIn

    sparkFilter.isInstanceOf[FilterType] should be (true)
    sparkFilter.isInstanceOf[SparkFilterType] should be (true)
    sparkFilter.isInstanceOf[AeroFilterType] should be (false)
  }

  it should "aeroFilter are sparkFilters" in {
    val aeroFilter: AeroFilterType = FilterString

    aeroFilter.isInstanceOf[FilterType] should be (true)
    aeroFilter.isInstanceOf[SparkFilterType] should be (true)
    aeroFilter.isInstanceOf[AeroFilterType] should be (true)
  }

  it should "be equals when same object" in {
    FilterNone shouldBe FilterNone
    FilterNone should not be FilterLong
    FilterNone should not be FilterLong
  }
}
