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

import com.aerospike.client.{Key, Record}
import com.aerospike.client.query.RecordSet

class RecordSetIteratorWrapper (val rs: RecordSet) extends java.util.Iterator[Record] with AutoCloseable{

  var fetched: Boolean = false
  var record : Record = null

  def next() : Record ={
    if(fetched)
    {
      fetched = false
      record
    }
    else
    {
      val b = rs.next()
      if(b)
      {
        record = rs.getRecord
        fetched = true
        record
      }
      else
        throw new Exception("Next called when no records are available")
    }
  }

  def hasNext : Boolean =
  {
    val b = rs.next()
    if(b)
    {
      record = rs.getRecord
      fetched = true
      true
    }
    else
      false
  }

  def remove() : Unit =
  {
    System.out.println("Iterator remove() called")
  }

  def close(): Unit =
  {
    //System.out.println("Iterator close() called")
    rs.close()
  }


}
