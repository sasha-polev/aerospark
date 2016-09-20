package com.aerospike.spark.sql

import org.scalatest.FlatSpec

import com.aerospike.client.AerospikeClient
import com.aerospike.client.AerospikeException
import com.aerospike.client.Bin
import com.aerospike.client.query.Statement
import com.aerospike.helper.query.KeyQualifier
import com.aerospike.helper.query.QueryEngine
import com.aerospike.client.Value
import com.aerospike.client.Key

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer

import scala.collection.JavaConversions.asScalaIterator
import com.aerospike.client.policy.WritePolicy



class QueyEngineTest extends FlatSpec{

  
	val ages = Array(25,26,27,28,29)
			val colours = Array("blue","red","yellow","green","orange")
			val animals = Array("cat","dog","mouse","snake","lion")
			
			val wp = new WritePolicy()
	    wp.expiration = 600

			var config: AerospikeConfig = _

			behavior of "QueryEngine from Scala"

			it should "Get a client from cache" in {
				config = AerospikeConfig.newConfig(Globals.seedHost, Globals.port, 20000)
				val client = AerospikeConnection.getClient(config)
			}

			it should "Get 3 clients and ensure ther are the same" in {
				val client1 = AerospikeConnection.getClient(config)
						val client2 = AerospikeConnection.getClient(config)
						assert(client1 == client2)
						val client3 = AerospikeConnection.getClient(config)
						assert(client1 == client3)
			}

			it should "Get a query engine from cache" in {
				val qe = AerospikeConnection.getQueryEngine(config)
			}

			it should "Insert data" in {
				val cl = AerospikeConnection.getClient(config)

						var i = 0
						for (x <- 1 to 100){
							val name = new Bin("name", "name:" + i);
							val age = new Bin("age", ages(i));
							val colour = new Bin("color", colours(i));
							val animal = new Bin("animal", animals(i));
							val key = new Key(Globals.namespace, "selector", "selector-test:"+ x)
									cl.put(null, key, name, age, colour, animal)
									i += 1
									if ( i == 5)
										i = 0
						}
			}

			it should "Select data" in {
				val qe = AerospikeConnection.getQueryEngine(config)
				val stmt = new Statement()
				stmt.setNamespace(Globals.namespace)
				stmt.setSetName("selector")
				var it = qe.select(stmt)
				for (row <- it) {
            val key = row.key
            val rec = row.record
            print(".")
        }
				println
				it.close()
			}
			it should "run concurrent selects" in {

			  var threads = new ListBuffer[Thread]()
				for (i <- 1 to 50) {
					val thread = new Thread {
						override def run {
							
							val qe = AerospikeConnection.getQueryEngine(config)
							val stmt = new Statement()
							stmt.setNamespace(Globals.namespace)
							stmt.setSetName("selector")
							println("\tStarted query " + i)
							var it = qe.select(stmt)
							var  count = 0
							try {
							while (it.hasNext()){
								val keyRecord = it.next()
										val key = keyRecord.key
										val record = keyRecord.record
										count = count + 1
							}
							} catch {
							  case ae:AerospikeException => println(ae)
							  case e:Exception => println(e)
							} 
							finally {
							  it.close()
							}
							println(s"\tCompleted Query $i with $count records")

						}

					}
				thread.start
				threads += thread
				}
			  for (t <- threads)
            t.join()
			  
			}
			it should "Select data by node" in {
			  val nodes = AerospikeConnection.getClient(config).getNodes
				
				var threads = new ListBuffer[Thread]()
				for (i <- 0 to nodes.length-1) {
				  val thread = new Thread {
						override def run {
						  val qe = AerospikeConnection.getQueryEngine(config)
				      val stmt = new Statement()
				      stmt.setNamespace(Globals.namespace)
				      stmt.setSetName("selector")
						  println("\tStarted partition " + i)
  				    var it = qe.select(stmt, false, nodes(i))
  				    var  count = 0
      				for (row <- it) {
                  val key = row.key
                  val rec = row.record
                  count = count + 1
              }
      				
      				it.close()
      				println(s"\tCompleted partition $i with $count records")
						}
				}
				thread.start
				threads += thread
			}
      for (t <- threads)
            t.join()
			  
			}


			it should "clean up because it's mother doesn't work here" in {
				val cl = AerospikeConnection.getClient(config)

						var i = 0
						for (x <- 1 to 100){
							val name = new Bin("name", "name:" + i);
							val age = new Bin("age", ages(i));
							val colour = new Bin("color", colours(i));
							val animal = new Bin("animal", animals(i));
							val key = new Key(Globals.namespace, "selector", "selector-test:"+ x)
									cl.delete(null, key)
									i += 1
									if ( i == 5)
										i = 0
						}
			}

}