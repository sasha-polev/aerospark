/*
 * Copyright 2014 OSSCube UK.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.osscube.spark.aerospike.rdd

import com.aerospike.client.AerospikeClient
import com.aerospike.client.cluster.Node
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.query.{Filter, RecordSet, Statement}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._


class AerospikeRDD(
                    @transient sc: SparkContext,
                    @transient aerospikeHosts: Array[Node],
                    val namespace: String,
                    val set: String,
                    val bins: Seq[String],
                    val filterType: Int,
                    val filterBin : String,
                    val filterStringVal: String,
                    @transient filterVals :  Seq[(Long, Long)]
                    ) extends BaseAerospikeRDD (sc, aerospikeHosts,  filterVals) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row]  = {

    val partition: AerospikePartition = split.asInstanceOf[AerospikePartition]
    val newSt = new Statement()
    newSt.setNamespace(namespace)
    newSt.setSetName(set)
    newSt.setBinNames(bins:_*)
    val aeroFilter: Filter = filterType match {
        case 0 => null
        case 1 => Filter.equal(filterBin, filterStringVal)
        case 2 => Filter.equal(filterBin, partition.startRange)
        case 3 => Filter.range(filterBin, partition.startRange, partition.endRange)
        case _ => null
    }
    if(aeroFilter != null)
      newSt.setFilters(aeroFilter)
    val endpoint = partition.endpoint
    logInfo("RDD: " + split.index + ", Connecting to: " + endpoint._1)
    val policy = new ClientPolicy()
    var res: RecordSet = null
    val client = new AerospikeClient(policy, endpoint._1, endpoint._2)

    res = client.queryNode(policy.queryPolicyDefault, newSt, client.getNode(endpoint._3))

    val wrapper: RecordSetIteratorWrapper = new RecordSetIteratorWrapper(res)
    context.addTaskCompletionListener(context => {wrapper.close(); client.close()})
    wrapper.asScala.map { p =>
        val binValues = bins.map(p._2.bins.get(_))
        Row.fromSeq(binValues)
      }
  }
}



object AerospikeRDD {

  def removeDoubleSpaces (s:String): String = if(!s.contains("  ")) s else removeDoubleSpaces(s.replace("  "," "))

  //Filter types: 0 none, 1 - equalsString, 2 - equalsLong, 3 - range
  /**
   * 
   * @param asql_statement
   * @param numPartitionsPerServerForRange
   * @return namespace, set, bins, filterType, filterBin, filterVals, filterStringVal
   */
  def parseSelect(asql_statement: String, numPartitionsPerServerForRange: Int): (String, String, Seq[String], Int, String, Seq[(Long, Long)] , String) = {

    if (asql_statement != null && !asql_statement.isEmpty) {
      val tokenised = removeDoubleSpaces(asql_statement.replace("=", " = ")).replace(", ", ",").replace(" ,", ",").split(" ")
      if (tokenised(0).toUpperCase != "SELECT")
        throw new Exception("Cant parse the statement, missing select: " + asql_statement)

      val bins = tokenised(1).split(",")

      if (tokenised(2).toUpperCase != "FROM")
        throw new Exception("Cant parse the statement, missing from: " + asql_statement)

      val namespaceAndSet = tokenised(3).split("\\.")
      val namespace = namespaceAndSet(0)
      val set = if (namespaceAndSet.length > 1) namespaceAndSet(1) else ""

      if (tokenised.length > 7 && tokenised(4).toUpperCase == "WHERE") {
        val positionOfBetween: Int = tokenised.map(_.toUpperCase).indexOf("BETWEEN")
        val positionOfAnd: Int = tokenised.map(_.toUpperCase).indexOf("AND")
        val positionOfEq: Int = tokenised.indexOf("=")
        val trimmedBin: String = tokenised(5).trim

        if (positionOfBetween == 6 && positionOfAnd == 8) {
          //Process range query here
          val lower: Long = tokenised(7).toLong
          val upper: Long = tokenised(9).toLong
          var tuples: Seq[(Long, Long)] = Seq((lower, upper))
          val range: Long = upper - lower
          if(numPartitionsPerServerForRange > 1 && range >= numPartitionsPerServerForRange) {
            val divided = range / numPartitionsPerServerForRange
            tuples =  (0 until numPartitionsPerServerForRange).map(i => (lower + divided*i , if(i == numPartitionsPerServerForRange -1) upper else lower + divided*(i +1) -1))
          }
          return (namespace, set, bins, 3, trimmedBin, tuples, "")
        }
        else if (positionOfEq == 6) {
          //Process equals query here
          if (tokenised(7).forall(n => n.isDigit || n == '-'))
            return (namespace, set, bins, 2, trimmedBin, Seq((tokenised(7).toLong, 0L)), "")
          else
            return (namespace, set, bins, 1, trimmedBin, Seq((0L, 0L)), tokenised(7))
        }
        else return (namespace, set, bins, 0, "", Seq((0L, 0L)), "")
      }
      else return (namespace, set, bins, 0, "", Seq((0L, 0L)), "")

    }
    null
  }
}
