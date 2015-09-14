package com.osscube.spark.aerospike

import com.osscube.spark.aerospike.rdd._

object AqlParser {

  def removeDoubleSpaces(s: String): String = if (!s.contains("  ")) s else removeDoubleSpaces(s.replace("  ", " "))

  def calculateRanges(lower: Long, upper: Long, partitions: Int): Seq[(Long, Long)] = {
    val range: Long = upper - lower
    val divided = range / partitions
    (0 until partitions)
      .map(i => (lower + divided * i,
      if (i == partitions - 1) upper
      else lower + divided * (i + 1) - 1))
  }

  private def checkParams(aqlStatement: String, partitions: Int) = {
    if (partitions < 1) throw new IllegalArgumentException("Partitions should be greater or equals 1 instead of " + partitions)
    if (aqlStatement == null || aqlStatement.trim.isEmpty) throw new IllegalArgumentException("Cant parse the statement: " + aqlStatement)
  }

  private def tokeniseQuery(aqlStatement: String): Array[String] = {
    val tokenised = removeDoubleSpaces(aqlStatement.replace("=", " = ")).replace(", ", ",").replace(" ,", ",").split(" ")
    if (tokenised(0).toUpperCase != "SELECT") throw new scala.IllegalArgumentException("Cant parse the statement, missing select: " + aqlStatement)
    if (tokenised(2).toUpperCase != "FROM") throw new scala.IllegalArgumentException("Cant parse the statement, missing from: " + aqlStatement)

    tokenised
  }

  /**
   *
   * @param aqlStatement ASQL statement to parse, select only
   * @param partitions number partitions per Aerospike snode
   * @return namespace, set, bins, filterType, filterBin, filterVals, filterStringVal
   */
  def parseSelect(aqlStatement: String, partitions: Int = 1): QueryParams = {
    checkParams(aqlStatement, partitions)

    val tokenised = tokeniseQuery(aqlStatement)

    val bins = tokenised(1).split(",")
    val namespaceAndSet = tokenised(3).split("\\.")
    val namespace = namespaceAndSet(0)
    val set = if (namespaceAndSet.length > 1) namespaceAndSet(1) else ""

    if (tokenised.length > 7 && tokenised(4).toUpperCase == "WHERE") {
      val filterBin: String = tokenised(5).trim

      if (isRangeQuery(tokenised)) {
        new QueryParams(namespace, set, bins, FilterRange, filterBin, processRange(tokenised, partitions), "")
      } else if (isPredicateQuery(tokenised)) {
        if (tokenised(7).forall(n => n.isDigit || n == '-'))
          new QueryParams(namespace, set, bins, FilterLong, filterBin, Seq((tokenised(7).toLong, 0L)), "")
        else
          new QueryParams(namespace, set, bins, FilterString, filterBin, Seq((0L, 0L)), tokenised(7))
      }
      else {
        throw new scala.IllegalArgumentException("Cant parse the statement, missing filter from WHERE: " + aqlStatement)
      }
    }
    else new QueryParams(namespace, set, bins, FilterNone, "", Seq((0L, 0L)), "")
  }

  private def isPredicateQuery(tokenised: Array[String]): Boolean = {
    tokenised.indexOf("=") == 6
  }

  private def isRangeQuery(tokenised: Array[String]): Boolean = {
    val positionOfBetween: Int = tokenised.map(_.toUpperCase).indexOf("BETWEEN")
    val positionOfAnd: Int = tokenised.map(_.toUpperCase).indexOf("AND")


    positionOfBetween == 6 && positionOfAnd == 8
  }

  private def processRange(tokenised: Array[String], partitions: Int): Seq[(Long, Long)] = {
    //Process range query here
    val lower: Long = tokenised(7).toLong
    val upper: Long = tokenised(9).toLong
    var tuples: Seq[(Long, Long)] = Seq((lower, upper))
    val range: Long = upper - lower
    if (partitions > 1 && range >= partitions) {
      tuples = calculateRanges(lower, upper, partitions)
    }

    tuples

  }
}
