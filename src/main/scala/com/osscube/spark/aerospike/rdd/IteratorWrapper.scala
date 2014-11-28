package com.osscube.spark.aerospike.rdd

import com.aerospike.client.query.RecordSet
import com.aerospike.client.Key
import com.aerospike.client.Record

class RecordSetIteratorWrapper (val rs: RecordSet) extends java.util.Iterator[(Key, Record)]{

  var fetched: Boolean = false
  var key: Key = null
  var record : Record = null

  def next() : (Key, Record) ={
    if(fetched)
    {
      fetched = false
      (key, record)
    }
    else
    {
      val b = rs.next()
      if(b)
      {
        key = rs.getKey
        record = rs.getRecord
        fetched = true
        (key, record)
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
      key = rs.getKey
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

}
