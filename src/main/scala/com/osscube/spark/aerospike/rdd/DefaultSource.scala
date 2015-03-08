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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

class DefaultSource extends RelationProvider {

  /**
   * Creates a new relation for Aerospike select statement.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    val partitionsPerServerPresent: Boolean = parameters.contains("partitionsPerServer") && !parameters("partitionsPerServer").isEmpty
    val useUdfWithoutIndexQueryPresent: Boolean = parameters.contains("useUdfWithoutIndexQuery") && !parameters("useUdfWithoutIndexQuery").isEmpty
    val useUdfWithoutIndexQuery = if (useUdfWithoutIndexQueryPresent) parameters("useUdfWithoutIndexQuery") == "true" else false
    if(partitionsPerServerPresent)
      AeroRelation(parameters("initialHost"), parameters("select"), parameters("partitionsPerServer").toInt, useUdfWithoutIndexQuery)(sqlContext)
    else
      AeroRelation(parameters("initialHost"), parameters("select"), 1, useUdfWithoutIndexQuery)(sqlContext)
  }
}