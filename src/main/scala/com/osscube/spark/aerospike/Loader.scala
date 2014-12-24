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

package com.osscube.spark.aerospike


import java.security.SecureRandom

import com.aerospike.client._
import com.aerospike.client.listener.WriteListener
import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.query.{RecordSet, Statement, Filter}
import com.aerospike.client.async.AsyncClient
import com.osscube.spark.aerospike.rdd.RecordSetIteratorWrapper
import scala.collection.JavaConverters._

object Loader {
  val random = new SecureRandom()
  def randomString(characterSet : String,  length: Int) :String = {
    val s = for  {i <-  0 until length
        randomCharIndex = random.nextInt(characterSet.length)
        result = characterSet(randomCharIndex)
    } yield result

    s.seq.mkString("")
  }


  def main(args: Array[String]) {


    val policy = new ClientPolicy()
    policy.failIfNotConnected = true
    //val client = new AsyncClient("192.168.142.162" , 3000)
    val client = new AerospikeClient("192.168.183.128" , 3000)

    val begin : Long = System.currentTimeMillis()
    val CHARSET_AZ_09 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

//    for(i <- 1 until 100000)
//    {
//      client.put( client.asyncWritePolicyDefault, new Key("test", "one_million", randomString(CHARSET_AZ_09, 5)),
//          new Bin("column1", randomString(CHARSET_AZ_09, 1)),
//          new Bin("column2", randomString(CHARSET_AZ_09, 2)),
//          new Bin("column3", randomString(CHARSET_AZ_09, 3)),
//          new Bin("column10", randomString(CHARSET_AZ_09, 10)),
//          new Bin("column50", randomString(CHARSET_AZ_09, 50)),
//          new Bin("column100", randomString(CHARSET_AZ_09, 100)),
//          new Bin("column300", randomString(CHARSET_AZ_09, 300)),
//          new Bin("longColumn1", random.nextLong),
//          new Bin("intColumn1", random.nextInt)
//      )
//
//      //Thread.sleep(1);
//    }

//    val udf_name: String = "spark_filters.lua"
//    if(!Info.request(client.getNodes()(0), "udf-list").contains("filename=" + udf_name)) {
//      val task = client.register(null, "udf/" + udf_name, udf_name, Language.LUA)
//      task.waitTillComplete()
//    }
//
//
//    val statement: Statement = new Statement()
//    statement.setNamespace("test")
//    statement.setSetName("one_million")
//    //statement.setBinNames("column1")
//    statement.setFilters(Filter.range("intColumn1", -100000000L, 100000000L))
//    statement.setAggregateFunction("spark_filters", "filter_in",  Array(Value.get("column1,column2,column3"), Value.get("column2"), Value.get("AA,BB")), true)
//    val recs: RecordSet = client.queryNode(client.queryPolicyDefault, statement, client.getNodes()(0))
//    println(recs.next())
//    println(recs.getRecord())
//    val w = new RecordSetIteratorWrapper(recs)
//    w.asScala.toArray.foreach { t =>
//      t.bins.get("SUCCESS") match {
//        //case m: java.util.HashMap[String , Any] => println(m)
//        case m : Any => println(m)
//      }
//    }

//    println(Info.request(client.getNodes()(0), "bins"))


    val end = System.currentTimeMillis()
    val seconds =  (end - begin) / 1000.0
    System.out.println(seconds)
    client.close()



    //System.out.println (parseSelect("select bin1 from namespace where bin2 between 0 and 2", 3))

  }


}

//class WriteHandler extends WriteListener {
//def onSuccess( key: Key) {
//}
//
//def onFailure(e: AerospikeException) {
//}
//}
